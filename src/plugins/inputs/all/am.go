//go:build !custom || inputs || inputs.apache

package all

import _ "github.com/influxdata/telegraf/plugins/inputs/am" // register plugin
