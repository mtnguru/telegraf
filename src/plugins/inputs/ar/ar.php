package ar


import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io/fs"

	//  automl "google.golang.org/genproto/googleapis/cloud/automl/v1beta1"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type Ar struct {
	DataTypes []string `toml:"dataTypes"`
	MoveFiles bool     `toml:"moveFiles"`
        MaxFiles int       `toml:"maxFiles"`
	MaxLines int       `toml:"maxLines"`
	FileName string    `toml:"fileName"`
	acc       telegraf.Accumulator
}

var exptName = "Exp 0931"
//var srcDir = "/apps/data/expt/201904130820"
var srcDir = "/home/sh/data"
var destDir = "/home/sh/data"

var dataTypes = []string{"GM"}            // Slobodan - GeigerMaster

var loc *time.Location
var err error

var ArConfig = `
  ## None
`

func (s *Ar) SampleConfig() string {
	return ArConfig
}

func (s *Ar) Description() string {
	return "Load GeigerMeister files"
}

/*
func listFiles() {
    var files []string

    err := filepath.Walk(rcDir, func(path string, info os.FileInfo, err error) error {
        files = append(files, path)
        return nil
    })
    if err != nil {
        panic(err)
    }
    for _, file := range files {
        fmt.Println(file)
    }
}
*/

func loadGeigerMaster(s *Ar, fs *os.File, filename string) error {
  fmt.Printf("  loadGeigerMeister %s\n", filename)
  dateLayout := "2.1.2006 15:4:5 CET"

  numLevels  := 6;
  numNodes   := 6;

  // metricId  GROUP_COMPONENT_$Node_$Level_CPS   GROUP_COMPONENT_NODE_LEVEL_UNITS
  //           E1_Gm_N2_L3_CPS
  group      := "E1";  // Electrolyzer 1
  component  := "GM";  // GeigerMeister
  units      := "CPS"; // Counts Per Second

  projectId  := "SH";  // Project SOHE
  instance   := "42";  // Project Instance
  metricType := "I";   // Metric Type - Input


  ln := 0
  scanner := bufio.NewScanner(fs)
  for scanner.Scan() {
    ln++
    txt := scanner.Text()
    cols := strings.Split(txt, ",")
    if ln > s.MaxLines {
	   return nil;
	}
    if cols[0] == "DATE" || cols[2] == "" {
      // ignore these lines
    } else {
      dateStamp := strings.TrimSpace(cols[0]) + " " +
            strings.TrimSpace(cols[1]+" CET")
                  dt, err := time.ParseInLocation(dateLayout, dateStamp, loc)
//    fmt.Printf("   startTime: %s   %s\n", dateStamp, dt.In(loc))
      if err != nil {
        fmt.Println(err)
      }
      if (ln % 10000) == 0 {
        fmt.Printf("     line |%d|\n", ln)
      }

      numSensors := numLevels * numNodes;
      for i := 0; i < numSensors; i++ {
        v := cols[i + 2];
//      fmt.Printf("     value |%s|\n", v)
        if v == "0" { continue; }
        fields := make(map[string]interface{});
        value, _ := strconv.Atoi(v);
        fields["value"]  = value;

        device   := "N" + strconv.Itoa(i / numLevels + 2);  // Node
        position := "L" + strconv.Itoa(i % numLevels + 1);  // Level

        metricId := group     + "_" +
                    component + "_" +
                    device    + "_" +
                    position  + "_" +
                    units

        tags := make(map[string]string)
        tags["ProjectId"] = projectId;
        tags["Instance"]  = instance;
        tags["Type"]      = metricType;

        tags["MetricId"]  = metricId;
        tags["Group"]     = group;
        tags["Component"] = component;
        tags["Device"]    = device;
        tags["Position"]  = position;
        //      tags["Units"]     = units;

        s.acc.AddFields(units, fields, tags, dt)
      }
    }
  }
  if err := scanner.Err(); err != nil {
    return err
  }
  return nil
}

func loadBolometer(s *Ar, fs *os.File, filename string) error {
	var dateI int = -1
	var timeI int = -1
	var powerI int = -1
	var irradianceI int = -1
	dateLayout := "1/02/2006 15:04:05 MST"
	ln := 0
	scanner := bufio.NewScanner(fs)
	header := true
	for scanner.Scan() {
		ln++
		txt := scanner.Text()
		cols := strings.Split(txt, ";")
		first := strings.TrimSpace(cols[0])

		if first == "Samples" {
			for i, v := range cols {
				// Ignore everything after the opening parantheses '('
				fields := strings.Split(v, " (")

				switch strings.TrimSpace(fields[0]) {
				case "Date":
					dateI = i
				case "Time of day":
					timeI = i
				case "Power":
					powerI = i
				case "Irradiance":
					irradianceI = i
				}
			}
			header = false
		} else if !header {
			dateStamp := strings.TrimSpace(cols[dateI]) + " " +
  				strings.TrimSpace(cols[timeI]+" CET")
			dt, err := time.ParseInLocation(dateLayout, dateStamp, loc)
//      	fmt.Printf("   startTime: %s   %s\n", dateStamp, dt.In(loc))
			if err != nil {
				fmt.Println(err)
			}
			fields := make(map[string]interface{})
			if powerI != -1 {
				power, _ := strconv.ParseFloat(cols[powerI], 64)
				fields["power"] = power
			}
			if irradianceI != -1 {
				irradiance, _ := strconv.ParseFloat(cols[irradianceI], 64)
				fields["irradiance"] = irradiance
    		}

			tags := make(map[string]string)
			tags["Experiment"] = exptName

			s.acc.AddFields("BOLO", fields, tags, dt)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

/** loadIR()

  Example data file:

    [Connect DataFile][1.1]
    Date:   2019-01-20
    Time:   14:46:06.876
    Unit:   °C
    Resolution:     0.037037037037037
    Values: 3
    Time    Area 1  Area 2  Area 3
    00:00:00.000    450.0   450.0   450.0
    00:00:00.037    450.0   450.0   450.0
**/
func loadIR(s *Ar, fs *os.File, filename string) error {
	fmt.Println("    loadIR - Filename: " + filename)
	var sdate, stime string
	var startTime time.Time
	var hcols []string
    var numCols int

	dateLayout := "2006-01-02 15:04:05.000 MST"
	ln := 0
	scanner := bufio.NewScanner(fs)
	header := true
	for scanner.Scan() {
		txt := scanner.Text()
		cols := strings.Split(txt, "\t")

		switch strings.TrimSpace(cols[0]) {
		case "Date:":
			sdate = cols[1]
		case "Time:":
			stime = cols[1]
		case "Time":
			fmt.Println("    Start Time: " + sdate + " " + stime + " CET")
			startTime, err = time.ParseInLocation(dateLayout, sdate+" "+stime+" CET", loc)
                        hcols = cols
                        numCols = len(hcols)
			fmt.Printf("     numCols %d",  numCols)

			if err != nil {
				fmt.Println(err)
			}
//      	fmt.Printf("     starttime = %s\n", startTime)
			header = false
		default:
			if ln < s.MaxLines && !header {
				if len(cols) != numCols {
					continue
				}
				ln++
				offset := cols[0]
//                 		fmt.Printf("    %d %d offset: %s\n", ln, len(cols), offset)
				of := strings.Split(offset, ":")
				mf := strings.Split(of[2], ".")
				var hr, mn, sc, ms int
				var ns int64
				hr, _ = strconv.Atoi(of[0])
				mn, _ = strconv.Atoi(of[1])
				sc, _ = strconv.Atoi(mf[0])
				ms, _ = strconv.Atoi(mf[1])
				ns = int64((hr*3600+mn*60+sc)*1e9 + ms*1e6)
//                              fmt.Printf("      ns: %d  %d  %d  %d\n", ns, hr, mn, sc)
				nt := startTime.UnixNano() + ns
				sec := nt / 1e9
				n := nt % 1e9
//              fmt.Printf("      nt: %d    s: %d    n: %d\n", nt, sec, n)
//       	    fmt.Printf("      %d:%d:%d\n", hr, mn, sc)

				dt := time.Unix(sec, n)
//      		fmt.Printf("      dt: %s\n", dt.In(loc))

				fields := make(map[string]interface{})

                for i, c := range hcols {
                    if i == 0 { continue }
				    a, _ := strconv.ParseFloat(cols[i], 64)
				    fields[c] = a
                }

				tags := make(map[string]string)
				tags["Experiment"] = exptName

//              fmt.Print("tags: ",tags,"\n")
                fmt.Print(dt.In(loc), " ",fields,"\n")
				s.acc.AddFields("IR", fields, tags, dt)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func loadDST(s *Ar, fs *os.File, filename string) error {
	fmt.Printf("\nloadDST()  %s\n", filename)
	var timeI, dateI int
	var dstI int
	var reading = false
	dateLayout := "2006-01-02 15:04:05.000 MST"
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	ln := 0
	fln := 0
	scanner := bufio.NewScanner(fs)
	for scanner.Scan() {
		txt := scanner.Text()
		fln++;
		cols := strings.Fields(txt)
		switch cols[0] {
		case "DATE":
			fmt.Printf("\nloadDST() Minute\n")
			reading = true

			// Assume header = DATE TIME DOY DST
			dateI = 0
			timeI = 1
			dstI = 3
		default:
			if ln < s.MaxLines && reading {
				ln++
				date := cols[dateI] + " " + cols[timeI] + " UTC"
				dt, _ := time.ParseInLocation(dateLayout, date, loc)
				fmt.Printf(" %d %d    %s   %s\n", fln, ln, date, dt.In(loc))

				dst, _ := strconv.ParseFloat(strings.ReplaceAll(cols[dstI], ",", ""), 64)

				fmt.Printf("      DST: %s\n",cols[dstI])
				fmt.Printf("      DST: %d\n",dst)

				fields := make(map[string]interface{})
				fields["DST"] = dst

				tags := make(map[string]string)
				tags["Experiment"] = exptName

				s.acc.AddFields("DST", fields, tags, dt)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func loadRad(s *Ar, fs *os.File, filename string) error {
	fmt.Printf("\nloadRad()  %s\n", filename)
	dateLayout := "01/02/06 3:04 PM MST"
	var hcols []string
	var timeI, dateI int
	var countI, totalI, avgI int
	var header = true
	ln := 0
	scanner := bufio.NewScanner(fs)
	for scanner.Scan() {
		txt := scanner.Text()
		cols := strings.Split(txt, "\t")
		switch cols[0] {
		case "Minute":
			fmt.Printf("\nloadRad() Minute\n")
			header = false
			hcols = cols
			for i, c := range hcols {
				switch c {
				case "Minute":
				case "Count":
					countI = i
				case "Total Counts":
					totalI = i
				case "Average Count":
					avgI = i
				case "Time":
					timeI = i
				case "Date":
					dateI = i
				case "Latitude":
				case "Longitude":
				case "Altitude":
				case "User Entered Data":
				}
			}
		default:
			if ln < s.MaxLines && !header {
				ln++
				date := cols[dateI] + " " + cols[timeI] + " CET"
				fmt.Println("    Date: " + date)
				dt, _ := time.ParseInLocation(dateLayout, date, loc)
				fmt.Printf("      dt: %s\n", dt.In(loc))

				count, _ := strconv.ParseInt(strings.ReplaceAll(cols[countI], ",", ""), 10, 64)
				total, _ := strconv.ParseInt(strings.ReplaceAll(cols[totalI], ",", ""), 10, 64)
				avg, _   := strconv.ParseFloat(strings.ReplaceAll(cols[avgI], ",", ""), 64)

				//              fmt.Printf("\n   Count: %d  Total: %d   Avg: %d\n",count, total, avg)

				fields := make(map[string]interface{})
				fields["Count"] = count
				fields["Total Counts"] = total
				fields["Average Counts"] = avg

				tags := make(map[string]string)
				tags["Experiment"] = exptName

				s.acc.AddFields("RAD100", fields, tags, dt)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func loadMoon(s *Ar, fs *os.File, filename string) error {
	fmt.Printf("\nloadMoon()  %s\n", filename)
	dateLayout := "2006-Jan-02 03:04 MST"
	var reading = false
	ln := 0
	scanner := bufio.NewScanner(fs)
	for scanner.Scan() {
		cols := strings.Fields(scanner.Text())
		if len(cols) == 0 {
			continue
		}
		col := strings.TrimSpace(cols[0])
		//      fmt.Println(ln, "   First Col ", col)

		switch col {
		case "Date__(UT)__HR:MN":
			fmt.Println("Header Line ", col)
			//          hcols = cols
		case "$$SOE":
			fmt.Println("SOE Line ", col)
			reading = true
		case "$$EOE":
			fmt.Println("EOE Line ", col)
			reading = false
		default:
			if ln < s.MaxLines && reading {
				ln++
				var long, lat float64
				if len(cols) == 17 {
					long, _ = strconv.ParseFloat(cols[15], 64)
					lat, _ = strconv.ParseFloat(cols[16], 64)
				} else if len(cols) == 18 {
					long, _ = strconv.ParseFloat(cols[16], 64)
					lat, _ = strconv.ParseFloat(cols[17], 64)
				} else {
					continue
				}
				fmt.Println(ln, "   Long and Lat ", long, " ", lat)

				date := cols[0] + " " + cols[1] + " CET"
				fmt.Println("    Date: " + date)
				dt, _ := time.ParseInLocation(dateLayout, date, loc)
				fmt.Printf("      dt: %s\n", dt.In(loc))

				fields := make(map[string]interface{})
				fields["Longitude"] = long
				fields["Latitude"] = lat

				tags := make(map[string]string)
				//             tags["Experiment"] = exptName

				s.acc.AddFields("moon", fields, tags, dt)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func loadMassSpec(s *Ar, fs *os.File, filename string) error {
	dateLayout := "01-02-2006 15:04:05.000 MST"
	var hcols []string

	ln := 0
	scanner := bufio.NewScanner(fs)
	for scanner.Scan() {
		ln++
		cols := strings.Split(scanner.Text(), ",")
		if ln == 1 {
			hcols = cols
		} else if ln < s.MaxLines {
			date := cols[0] + " " + cols[1] + " CET"
			fmt.Println("    Date: " + date)
			dt, _ := time.ParseInLocation(dateLayout, date, loc)
			fmt.Printf("      dt: %s\n", dt.In(loc))
			for i, c := range cols {
				h := strings.TrimSpace(hcols[i])
				switch h {
				case "Date":
				case "Time":
				case "Ctrlr Timestamp":
				case "Low AMU":
				case "High AMU":
				case "Inst. Total Pressure":
				case "Avg Total Pressure":
				case "Average Mode":
				case "Number Averaged":
				case "Error Count":
				case "Total Area":
				case "Noise Baseline":
				case "High Mass Area":
				default:
					c = strings.TrimSpace(c)
					if c != "0.0000E+0" {
						fmt.Printf("    %s  c: |%s|\n", h, c)
						fields := make(map[string]interface{})
						val, _ := strconv.ParseFloat(c, 64)
						fields["Value"] = val

						tags := make(map[string]string)
						tags["Experiment"] = exptName
//				                tags["AMU"] = fmt.Sprintf("%02d",h)
                                                if len(h) == 1 {
                                                    h = "0" + h
                                                }
						tags["AMU"] = h

						s.acc.AddFields("rga", fields, tags, dt)
					}
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

///// Load SCADA data

type Sensor struct {
	metric string
	device string
	num  string
	id  string
}
type Sensors map[string]Sensor

func readSensors()(Sensors) {
//  fmt.Printf("readSensors start\n")
	sensors := make(Sensors)

	records, err := readCSV("/apps/telegraf/config/scada.csv")

	if err != nil {
        fmt.Printf("fatal error reading sensors: %s\n", err)
		log.Fatal(err)
	}

//  fmt.Printf("process records\n")
	for _, record := range records {
		id := fmt.Sprintf("%s-%s", record[0], record[1])
		fmt.Println("   add sensor: ", id)
		sensor := Sensor{
			device:  record[0],
			num:     record[1],
			metric:    record[3],
			id:      id,
		}
		sensors[id] = sensor
//      fmt.Printf("%s %s %s %s\n", sensor.id, sensor.name, sensor.device, sensor.num)
	}
	return sensors
}

func readCSV(fileName string) ([][]string, error) {
//  fmt.Printf("readCSV Start\n")
	f, err := os.Open(fileName)

	if err != nil {
		return [][]string{}, err
	}

	defer f.Close()

	r := csv.NewReader(f)

	// skip first line
	if _, err := r.Read(); err != nil {
		return [][]string{}, err
	}

	records, err := r.ReadAll()

	if err != nil {
  		fmt.Println("ReadAll error: ", err)
		return [][]string{}, err
	}

	return records, nil
}


func makeTags(metric string)(map[string]string) {
    fmt.Printf("makeTags - enter %s\n", metric)
    tags := make(map[string]string)

    // Separate the metric
    f := strings.Split(metric, "_")
	nf := len(f)

	tags["Metric"] = metric

	if nf >= 3 {
		tags["Type"] = f[0]
		tags["Component"] = f[1]
		tags["Units"] = f[nf - 1]
	}
	if nf >= 4 {
		tags["Device"] = f[2]
	}
	if nf >= 5 {
		tags["Position"] = f[3]
	}
	if nf >= 6 {
		tags["Composition"] = f[4]
	}
    return tags
}

func loadScada(s *Ar, fs *os.File, filename string, edate string) error {
	fmt.Printf("    loadScada filename %s\n",filename)
	dateLayout := "20060102 150405.000"

	edate = edate[0:8]
//  fileDateLayout := "20060102 MST"
//  dt, _ := time.ParseInLocation(fileDateLayout, edate + " CET", loc)
//   fmt.Printf("     %s fileDate: %s\n", edate, dt.In(loc))
    sensors := readSensors()
    fmt.Println("   sensors", sensors)

    dataq := strings.Split(filename, ".")[0]
	fmt.Printf("   loadScada - dataq   %s\n", dataq)

    var names[8] string
    names[0] = dataq + "-1"
	names[1] = dataq + "-2"
	names[2] = dataq + "-3"
	names[3] = dataq + "-4"
	names[4] = dataq + "-5"
	names[5] = dataq + "-6"
	names[6] = dataq + "-7"
	names[7] = dataq + "-8"

	ln := 0
	scanner := bufio.NewScanner(fs)
	for scanner.Scan() {
		ln++
		if scanner.Text() == "scada_log" { continue }
		cols := strings.Split(scanner.Text(), ",")
		if ln < s.MaxLines {
			t := cols[0][0:6] + "." + cols[0][6:9]
			date := edate + " " + t
//     		date := fileDate + " " + cols[0] + " CET"
  			fmt.Println("    Date: " + date)
			dt, err := time.ParseInLocation(dateLayout, date, loc)
			if err != nil {
			    fmt.Printf("FATAL ERROR: %s\n", err)
				log.Fatal(err)
			}
  			fmt.Printf("    %d   %s  dt: %s\n", dt.UnixNano(), date, dt.In(loc))
			for i, c := range cols {
				if i == 0 { continue }
				c = strings.TrimSpace(c)

  			    fmt.Printf("    Name  %s \n", names[i-1])
  			    sensor := sensors[names[i-1]]
  			    fmt.Println("    sensor \n", sensor)
				metric := sensors[names[i-1]].metric
  			    fmt.Println("    metric \n", metric)
				tags := makeTags(metric)
				tags["Experiment"] = exptName
				tags["DataQ"] = names[i-1]

				units := tags["Units"]
				if units == "" { units = "Default" }

				fields := make(map[string]interface{})
				if units == "Open" {
					//					fmt.Printf("    %d  c: |%s|\n", i, c)
					if c == "Open" {
						fields["Status"] = "1"
					} else if c == "Closed" {
						fields["Status"] = "1"
					} else {
						fields["Status"] = "-1"
					}
					if ln == 1 {
						fmt.Println("    Status: ", i, sensors[names[i-1]].metric, fields["Status"])
					}
				} else if units == "Closed" {
					//					fmt.Printf("    %d  c: |%s|\n", i, c)
					if c == "Closed" {
						fields["Status"] = "0"
					} else if c == "Open" {
						fields["Status"] = "0"
					} else {
						fields["Status"] = "-1"
					}
					if ln == 1 {
						fmt.Println("    Status: ", i, sensors[names[i-1]].metric, fields["Status"])
					}
				} else {
					fmt.Printf("    %d  c: |%s|\n", i, c)
					val, _ := strconv.ParseFloat(c, 64)
					fields["Value"] = val
					if ln != 0 {
						fmt.Println("    Value: ", i, sensors[names[i-1]].metric, fields["Value"])
					}
				}

				var meas string
				if (tags["Units"] != "") {
					meas = tags["Units"]
				} else {
					meas = "Default"
				}

   			    fmt.Println("    fields: ", tags, fields)
				s.acc.AddFields(meas, fields, tags, dt)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

var picoStartTime time.Time
var picoLast[10]float64

func loadPicoscope(s *Ar, fs *os.File, filename string, dataType string) error {
	var hcols []string
	var nhcols int

    if picoStartTime.IsZero() {
        startLayout := "2006-01-02 15:04:05 MST"
	    fmt.Println("     read starttime")
        filePath := srcDir + "/" + dataType + "/starttime.txt"
		tfs, err := os.Open(filePath)
        if err != nil {
			log.Fatal(err)
        }
		defer tfs.Close()
	    scanner := bufio.NewScanner(tfs)
	    scanner.Scan()
        st := scanner.Text()
//      fmt.Println("     read: " + st)
	    picoStartTime, err = time.ParseInLocation(startLayout, st, loc)
        if err != nil {
    		log.Fatal(err)
	    }
// 	    fmt.Printf("   startTime: %s   %s\n", st, picoStartTime.In(loc))
    }
	fmt.Printf("     starttime = %s\n", picoStartTime)

	ln := 0
	scanner := bufio.NewScanner(fs)
	header := true
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), ",")


		switch strings.TrimSpace(cols[0]) {
		case "Time":
            hcols = cols
            nhcols = len(hcols)
			fmt.Printf("         ignore\n")
		case "(s)":
			header = false
		default:
			if ln < s.MaxLines && !header {
				if len(cols) != nhcols {
					continue
				}
				ln++

                // Add in the starttime
				ns,_ := strconv.ParseFloat(cols[0], 64)
				nt := picoStartTime.UnixNano() + int64(ns * 1000000000.0)
				sec := nt / 1e9
				n := nt % 1e9
//       		fmt.Printf("      nt: %d    sec: %d    ns: %d  nns: %d\n", nt, sec, ns, n)

				dt := time.Unix(sec, n)
				fmt.Printf("      dt: %s\n", dt.In(loc))

			    for i, _ := range cols {
                    if i == 0 {continue}
				    val, err := strconv.ParseFloat(cols[i], 64)
                    if err != nil {
                       	log.Fatal(err)
                    }
//                  fmt.Printf("   DUDETTE:  %g\n", picoLast[i])
                    if val == picoLast[i] {
//                      fmt.Printf("     line: %d  %d   %s   same\n", ln, i, val)
                    } else {
//                      fmt.Printf("     line: %d  %d   %s\n", ln, i, val)
                        picoLast[i] = val

			  	        fields := make(map[string]interface{})
				        fields[hcols[i]] = val

//           		    tags := make(map[string]string)
// 		                tags["Experiment"] = exptName

				        s.acc.AddFields("picoscope", fields, nil, dt)
                    }
                }
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func loadDataType(s *Ar, dataType string) {
	dirPath := srcDir + "/" + dataType
	fmt.Printf("loadDataType - dirPath: %s\n", dirPath)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
	    fmt.Printf("loadDataType - ERROR:  %s\n", err)
		return
	}

	fmt.Printf("ReadDir: %s\n", dirPath)
	var files []fs.FileInfo
        files, err = ioutil.ReadDir(dirPath)
        if err != nil {
            log.Fatal(err)
        }

	nfiles := len(files)
	fmt.Printf("\nLoad Datatype: %2d - %s\n", nfiles, dataType)
	fn := 0
	for _, f := range files {
		if fn > s.MaxFiles - 1 {
			continue
		}
		var filename = f.Name()
  		fmt.Printf("   File %d   %s    %s\n", fn, s.FileName, filename)
		if s.FileName != "" && s.FileName != filename {
			fmt.Printf("  Ignore File %d   %s\n", fn, filename)
  			continue
		}
        srcFile := dirPath + "/" + filename
		fs, err2 := os.Open(srcFile)
		if err2 != nil {
			log.Fatal(err)
		}
		defer fs.Close()

		ss := strings.Split(filename, ".")
		suffix := ss[len(ss)-1]

		fmt.Printf("  Load File %d   %s  %s   %s    %s\n", fn, filename, suffix, dataType, srcFile)

		var err3 error
		switch dataType {
                case "GM": // Geiger Master
			if suffix == "CSV" {
		                fmt.Printf("  do it\n");
				fn++
				err3 = loadGeigerMaster(s, fs, filename)
			}
		case "BOLO":
			if suffix == "csv" {
				fn++
				err3 = loadBolometer(s, fs, filename)
			}
		case "IR":
			if suffix == "dat" {
				fn++
				err3 = loadIR(s, fs, filename)
			}
		case "pwr":
			if suffix == "csv" {
				fn++
				err3 = loadPicoscope(s, fs, filename, dataType)
			}
		case "DST":
			if suffix == "txt" {
				fn++
				err3 = loadDST(s, fs, filename)
			}
		case "RAD100":
			if suffix == "txt" {
				fn++
				err3 = loadRad(s, fs, filename)
			}
		case "moon":
			if suffix == "txt" {
				fn++
				err3 = loadMoon(s, fs, filename)
			}
		case "MS SPEC":
                    fallthrough
		case "MASS SPEC":
			if suffix == "csv" {
				fn++
				err3 = loadMassSpec(s, fs, filename)
			}
		case "SCADA":
//               	if suffix == "txt" && filename != "scada.txt" {
//               		fn++
//               		err3 = loadScada(s, fs, filename, edate)
//               	}
		}

		if err3 != nil {
			log.Fatal(err3)
			if s.MoveFiles {
				moveFile(srcFile, "error", exptName, dataType, filename)
			}
		} else {
			if s.MoveFiles {
				moveFile(srcFile, "archive", exptName, dataType, filename)
			}
		}
	}
}

func moveFile(srcFile, status, exptName, dataType, filename string) {

	// Create the directory destDir/status/exptName
	dest := destDir + "/" + status + "/" + exptName
	if _, err := os.Stat(dest); errors.Is(err, os.ErrNotExist) {
		err2 := os.Mkdir(dest, os.ModePerm)
		if err2 != nil {
			fmt.Println("Could not create directory: " + dest)
		}
	}

	// Create the directory destDir/status/exptName/dataType
	dest = dest + "/" + dataType
	if _, err := os.Stat(dest); errors.Is(err, os.ErrNotExist) {
		err2 := os.Mkdir(dest, os.ModePerm)
		if err2 != nil {
			fmt.Println("Could not create directory: " + dest)
		}
	}
	destFile := dest + "/" + filename
	fmt.Printf("  moveFile %s -> %s\n", srcFile, destFile)
	err := os.Rename(srcFile,destFile)
	if err != nil {
		fmt.Println("Could not move file: " + srcFile + " -> " + destFile)
		fmt.Println(err)
	}
}

/*
   List all directories in srcDir

   Directories
     input
       New raw files are placed here by the source
       A prep processor extracts information needed for loading
       A starlark processor edits data lines
       A dispatch processor
         Completes the loading
         If successful
           Move raw file to archive directory
           Processed file is stored in input directory
         Not successful
           Move raw file to error directory - along with error file

     load - May not need this directory
       Raw files that have through the starlark processor

     error
       All files that cannot be processed

     archive
       Copies of raw files are placed here for long term archival
*/
func (s *Ar) Gather(acc telegraf.Accumulator) error {
	srcDir, err = os.Getwd()
    fmt.Println("Gather - srcDir", srcDir)
	if err != nil { log.Println(err) }
	fmt.Println("Present working directory " + srcDir)
	if loc, err = time.LoadLocation("CET"); err != nil {
		log.Fatal(err)
	}
	s.acc = acc

	for _, dataType := range s.DataTypes {
		loadDataType(s, dataType)
	}

	return nil
}

func init() {
	//  inputs.Add("ar", func() telegraf.Input { return &Ar{x: 0.0} })
//  inputs.Add("ar", func() telegraf.Input { return &Ar{x: 0.0} })
	inputs.Add("ar", func() telegraf.Input { return &Ar{} })
}