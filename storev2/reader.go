package storev2

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/jaeger-influxdb/common"
	"github.com/influxdata/jaeger-influxdb/dbmodel"
	"github.com/influxdata/jaeger-influxdb/influx2http"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var _ spanstore.Reader = (*Reader)(nil)

// Reader can query for and load traces from InfluxDB v2.x.
type Reader struct {
	fluxQueryService    *influx2http.FluxQueryService
	orgID               influxdb.ID
	bucket              string
	spanMeasurement     string
	spanMetaMeasurement string
	logMeasurement      string
	defaultLookback     time.Duration

	resultDecoder *csv.ResultDecoder

	logger hclog.Logger
}

// NewReader returns a new SpanReader for InfluxDB v2.x.
func NewReader(fluxQueryService *influx2http.FluxQueryService, orgID influxdb.ID, bucket, spanMeasurement, spanMetaMeasurement, logMeasurement string, defaultLookback time.Duration, logger hclog.Logger) *Reader {
	return &Reader{
		resultDecoder:       csv.NewResultDecoder(csv.ResultDecoderConfig{}),
		fluxQueryService:    fluxQueryService,
		orgID:               orgID,
		bucket:              bucket,
		spanMeasurement:     spanMeasurement,
		spanMetaMeasurement: spanMetaMeasurement,
		logMeasurement:      logMeasurement,
		defaultLookback:     defaultLookback,
		logger:              logger,
	}
}

func (r *Reader) query(ctx context.Context, fluxQuery string) (flux.ResultIterator, error) {
	r.logger.Warn(fluxQuery)
	request := &query.Request{
		OrganizationID: r.orgID,
		Compiler:       lang.FluxCompiler{Query: fluxQuery},
	}

	return r.fluxQueryService.Query(ctx, request)
}

const queryGetServicesFlux = `
import "influxdata/influxdb/v1"
v1.measurementTagValues(bucket: "%s", measurement: "%s", tag: "%s")
`

// GetServices returns all services traced by Jaeger
func (r *Reader) GetServices(ctx context.Context) ([]string, error) {
	r.logger.Warn("GetServices called")

	resultIterator, err := r.query(ctx, fmt.Sprintf(queryGetServicesFlux, r.bucket, r.spanMetaMeasurement, common.ServiceNameKey))
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, err
	}
	var services []string
	for resultIterator.More() {
		err = resultIterator.Next().Tables().Do(func(table flux.Table) error {
			return table.Do(func(reader flux.ColReader) error {
				for rowI := 0; rowI < reader.Len(); rowI++ {
					services = append(services, reader.Strings(0).ValueString(rowI))
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}

	return services, nil
}

const queryGetOperationsFlux = `
import "influxdata/influxdb/v1"
v1.tagValues(bucket:"%s", tag:"%s", predicate: (r) => r._measurement=="%s" and r.%s=="%s")
`

// GetOperations returns all operations for a specific service traced by Jaeger
func (r *Reader) GetOperations(ctx context.Context, param spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	r.logger.Warn("GetOperations called")

	q := fmt.Sprintf(queryGetOperationsFlux, r.bucket, common.OperationNameKey, r.spanMetaMeasurement, common.ServiceNameKey, param.ServiceName)
	resultIterator, err := r.query(ctx, q)
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, err
	}

	var operations []spanstore.Operation
	for resultIterator.More() {
		err = resultIterator.Next().Tables().Do(func(table flux.Table) error {
			return table.Do(func(reader flux.ColReader) error {
				for rowI := 0; rowI < reader.Len(); rowI++ {
					operations = append(operations, spanstore.Operation{
						Name:     reader.Strings(0).ValueString(rowI),
						SpanKind: param.SpanKind,
					})
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}

	return operations, nil
}

// GetTrace takes a traceID and returns a Trace associated with that traceID
func (r *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	r.logger.Warn("GetTrace called")

	result, err := r.query(ctx,
		dbmodel.NewFluxTraceQuery(r.bucket, r.spanMeasurement, r.logMeasurement, time.Now().Add(r.defaultLookback)).BuildTraceQuery([]model.TraceID{traceID}))
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, err
	}

	traces, err := dbmodel.TracesFromFluxResult(result, r.spanMeasurement, r.logMeasurement, r.logger)
	if err != nil {
		return nil, err
	}
	if len(traces) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}
	if len(traces) > 1 {
		panic("more than one trace returned, expected exactly one; bug in query?")
	}

	return traces[0], nil
}

// FindTraces retrieve traces that match the traceQuery
func (r *Reader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	r.logger.Warn("FindTraces called")

	traceIDs, err := r.FindTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(traceIDs) == 0 {
		return nil, nil
	}

	tq := dbmodel.NewFluxTraceQuery(r.bucket, r.spanMeasurement, r.logMeasurement, query.StartTimeMin)
	if !query.StartTimeMax.IsZero() {
		tq.StartTimeMax(query.StartTimeMax)
	}
	result, err := r.query(ctx, tq.BuildTraceQuery(traceIDs))
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, err
	}
	traces, err := dbmodel.TracesFromFluxResult(result, r.spanMeasurement, r.logMeasurement, r.logger)
	if err != nil {
		return nil, err
	}

	return traces, nil
}

// FindTraceIDs retrieve traceIDs that match the traceQuery
func (r *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	r.logger.Warn("FindTraceIDs called")

	q := dbmodel.FluxTraceQueryFromTQP(r.bucket, r.spanMeasurement, r.logMeasurement, query)
	result, err := r.query(ctx, q.BuildTraceIDQuery())
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, err
	}

	return dbmodel.TraceIDsFromFluxResult(result)
}

var getDependenciesQueryFlux = fmt.Sprintf(`
from(bucket: "%%s")
 |> range(start: %%s, stop: %%s)
 |> filter(fn: (r) => r._measurement == "%%s" and (r._field == "%s" or r._field == "%s"))
 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
 |> group()
 |> keep(columns: ["%s", "%s", "%s"])
`, "span_id", "references", "span_id", "references", "service_name")

// GetDependencies returns all inter-service dependencies
func (r *Reader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	r.logger.Warn("GetDependencies called")

	resultIterator, err := r.query(ctx,
		fmt.Sprintf(getDependenciesQueryFlux,
			r.bucket, endTs.Add(-1*lookback).UTC().Format(time.RFC3339Nano), endTs.UTC().Format(time.RFC3339Nano), r.spanMeasurement))
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return nil, err
	}

	return dbmodel.DependencyLinksFromResultV2(resultIterator)
}
