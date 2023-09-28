package ar

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

var sampleConfig = `
  ## Remove the host tag

  # [processors.ar.tags]
  #   additional_tag = "tag_value"
`

type Ar struct {
	NameOverride string
	NamePrefix   string
	NameSuffix   string
	Tags         map[string]string
}

func (c *Ar) SampleConfig() string {
	return sampleConfig
}

func (c *Ar) Description() string {
	return "Remove the host tag."
}

func (c *Ar) Apply(in ...telegraf.Metric) []telegraf.Metric {
        for _, metric := range in {
                metric.RemoveTag("host")
        }
	return (in)
}

func init() {
	processors.Add("ar", func() telegraf.Processor {
		return &Ar{}
	})
}
