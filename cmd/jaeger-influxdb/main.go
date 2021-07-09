package main

import (
	"flag"
	"os"
	"sort"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/influxdata/jaeger-influxdb/config"
	"github.com/influxdata/jaeger-influxdb/storev1"
	"github.com/influxdata/jaeger-influxdb/storev2"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var configPath string

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  "jaeger-influxdb",
		Level: hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
	})

	flag.StringVar(&configPath, "config", "", "The absolute path to the InfluxDB plugin's configuration file")
	flag.Parse()

	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	if configPath != "" {
		v.SetConfigFile(configPath)

		err := v.ReadInConfig()
		if err != nil {
			logger.Error("failed to parse configuration file", "error", err)
			os.Exit(1)
		}
	}

	conf := config.Configuration{}
	conf.InitFromViper(v)

	environ := os.Environ()
	sort.Strings(environ)
	for _, env := range environ {
		logger.Warn(env)
	}

	var store shared.PluginServices
	var closeStore func() error
	var err error

	if conf.Database != "" {
		logger.Warn("Started with InfluxDB v1")

		store.Store, closeStore, err = storev1.NewStore(&conf, logger)
	} else if conf.Organization != "" && conf.Bucket != "" && conf.Token != "" {
		logger.Warn("Started with InfluxDB v2")
		store.Store, closeStore, err = storev2.NewStore(&conf, logger)
	} else {
		err = errors.New("missing flags; for InfluxDB V1 set database and retention policy; for InfluxDB V2 set organization, bucket and token")
	}

	if err != nil {
		logger.Error("failed to open store", "error", err)
		os.Exit(1)
	}

	grpc.Serve(&store)

	if err = closeStore(); err != nil {
		logger.Error("failed to close store", "error", err)
		os.Exit(1)
	}
}
