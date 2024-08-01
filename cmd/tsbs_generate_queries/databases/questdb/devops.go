package questdb

import (
	"fmt"
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/taosdata/tsbs/pkg/data/usecases/common"
	"slices"
	"strings"
	"time"

	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/taosdata/tsbs/pkg/query"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces QuestDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getSelectAggClauses builds specified aggregate function clauses for
// a set of column idents.
//
// For instance:
//
//	max(cpu_time) AS max_cpu_time
func (d *Devops) getSelectAggClauses(aggFunc string, idents []string) []string {
	selectAggClauses := make([]string, len(idents))
	for i, ident := range idents {
		selectAggClauses[i] =
			fmt.Sprintf("%[1]s(%[2]s) AS %[1]s_%[2]s", aggFunc, ident)
	}
	return selectAggClauses
}

func (d *Devops) getHostWhereStringAndTagString(metric string, nHosts int) (string, string) {
	hostnames, err := d.GetRandomHosts(nHosts)
	databases.PanicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames), d.getTagStringWithNames(metric, hostnames)
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	var hostnameClauses []string
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	}
	return fmt.Sprintf("hostname IN (%s)", strings.Join(hostnameClauses, ","))
}

func (d *Devops) getTagStringWithNames(metric string, names []string) string {
	tagString := ""
	tagString += "{"
	slices.Sort(names)
	for _, s := range names {
		tagString += fmt.Sprintf("(%s.hostname=%s)", metric, s)
	}
	tagString += "}"
	return tagString
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for N random
// hosts
//
// Queries:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)
	selectClauses := d.getSelectAggClauses("max", devops.GetAllCPUMetrics())
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	sql := fmt.Sprintf(`
		SELECT
			hour(timestamp) AS hour,
			%s
		FROM cpu
		WHERE hostname IN ('%s')
		  AND timestamp >= '%s'
		  AND timestamp < '%s'
		SAMPLE BY 1h`,
		strings.Join(selectClauses, ", "),
		strings.Join(hosts, "', '"),
		interval.StartString(),
		interval.EndString())

	humanLabel := devops.GetMaxAllLabel("QuestDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of metrics in the group `cpu` per device
// per hour for a day
//
// Queries:
// double-groupby-1
// double-groupby-5
// double-groupby-all
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	selectClauses := d.getSelectAggClauses("avg", metrics)

	sql := fmt.Sprintf(`
		SELECT timestamp, hostname,
			%s
		FROM cpu
		WHERE timestamp >= '%s'
		  AND timestamp < '%s'
		SAMPLE BY 1h
		GROUP BY timestamp, hostname`,
		strings.Join(selectClauses, ", "),
		interval.StartString(),
		interval.EndString())

	humanLabel := devops.GetDoubleGroupByLabel("QuestDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause,
// that groups by a truncated date, orders by that date, and takes a limit:
//
// Queries:
// groupby-orderby-limit
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	sql := fmt.Sprintf(`
		SELECT timestamp AS minute,
			max(usage_user)
		FROM cpu
		WHERE timestamp < '%s'
		SAMPLE BY 1m
		LIMIT 5`,
		interval.EndString())

	humanLabel := "QuestDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
//
// Queries:
// lastpoint
func (d *Devops) LastPointPerHost(qi query.Query) {
	sql := fmt.Sprintf(`SELECT * FROM cpu latest by hostname`)

	humanLabel := "QuestDB last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has
// high usage between a time period for a number of hosts (if 0, it will
// search all hosts)
//
// Queries:
// high-cpu-1
// high-cpu-all
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	sql := ""
	if nHosts > 0 {
		hosts, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)

		sql = fmt.Sprintf(`
		      SELECT *
		      FROM cpu
		      WHERE usage_user > 90.0
		       AND hostname IN ('%s')
		       AND timestamp >= '%s'
		       AND timestamp < '%s'`,
			strings.Join(hosts, "', '"),
			interval.StartString(),
			interval.EndString())
	} else {
		sql = fmt.Sprintf(`
		      SELECT *
		      FROM cpu
		      WHERE usage_user > 90.0
		       AND timestamp >= '%s'
		       AND timestamp < '%s'`,
			interval.StartString(),
			interval.EndString())
	}

	humanLabel, err := devops.GetHighCPULabel("QuestDB", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByTime selects the MAX for metrics under 'cpu', per minute for N random
// hosts
//
// Resultsets:
// single-groupby-1-1-12
// single-groupby-1-1-1
// single-groupby-1-8-1
// single-groupby-5-1-12
// single-groupby-5-1-1
// single-groupby-5-8-1
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectAggClauses("max", metrics)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	sql := fmt.Sprintf(`
		SELECT timestamp,
			%s
		FROM cpu
		WHERE hostname IN ('%s')
		  AND timestamp >= '%s'
		  AND timestamp < '%s'
		SAMPLE BY 1m`,
		strings.Join(selectClauses, ", "),
		strings.Join(hosts, "', '"),
		interval.StartString(),
		interval.EndString())

	humanLabel := fmt.Sprintf(
		"QuestDB %d cpu metric(s), random %4d hosts, random %s by 1m",
		numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// New Queries
func (d *Devops) ThreeField1(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT timestamp,hostname,avg(usage_user),avg(usage_system),avg(usage_idle) FROM cpu WHERE %s AND timestamp >= '%s' AND timestamp < '%s' SAMPLE BY %s`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "QuestDB Three Field 1"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *Devops) ThreeField2(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT timestamp,hostname,avg(usage_idle),avg(usage_nice),avg(usage_iowait) FROM cpu WHERE %s AND timestamp >= '%s' AND timestamp < '%s' SAMPLE BY %s`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_idle[int64],usage_nice[int64],usage_iowait[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "QuestDB Three Field 2"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *Devops) ThreeField3(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT timestamp,hostname,avg(usage_system),avg(usage_idle),avg(usage_nice) FROM cpu WHERE %s AND timestamp >= '%s' AND timestamp < '%s' SAMPLE BY %s`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "QuestDB Three Field 3"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *Devops) FiveField1(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT timestamp,hostname,avg(usage_user),avg(usage_system),avg(usage_idle),avg(usage_nice),avg(usage_iowait) FROM cpu WHERE %s AND timestamp >= '%s' AND timestamp < '%s' SAMPLE BY %s`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "QuestDB Five Field 1"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *Devops) TenField(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := ""
	if zipNum < 5 {
		duration = "15m"
	} else {
		duration = "60m"
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT timestamp,hostname,avg(usage_user),avg(usage_system),avg(usage_idle),avg(usage_nice),avg(usage_iowait),avg(usage_irq),avg(usage_softirq),avg(usage_steal),avg(usage_guest),avg(usage_guest_nice) FROM cpu WHERE %s AND timestamp >= '%s' AND timestamp < '%s' SAMPLE BY %s`,
		hostWhereString, interval.StartString(), interval.EndString(), duration)

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64],usage_irq[int64],usage_softirq[int64],usage_steal[int64],usage_guest[int64],usage_guest_nice[int64]}#{empty}#{mean,%s}", tagString, duration)

	humanLabel := "QuestDB Ten Field "
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *Devops) TenFieldWithPredicate(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT timestamp,hostname,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM cpu WHERE %s AND timestamp >= '%s' AND timestamp < '%s' AND usage_user > 90 AND usage_guest > 90`,
		hostWhereString, interval.StartString(), interval.EndString())

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64],usage_irq[int64],usage_softirq[int64],usage_steal[int64],usage_guest[int64],usage_guest_nice[int64]}#{(usage_user>90[int64])(usage_guest>90[int64])}#{empty,empty}", tagString)

	humanLabel := "QuestDB Ten Field with Predicate"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

var index = 0

func (d *Devops) CPUQueries(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	switch index % 6 {
	case 0:
		d.ThreeField3(qi, zipNum, latestNum, newOrOld)
		break
	case 1:
		d.FiveField1(qi, zipNum, latestNum, newOrOld)
		break
	case 2:
		d.ThreeField1(qi, zipNum, latestNum, newOrOld)
		break
	case 3:
		d.ThreeField2(qi, zipNum, latestNum, newOrOld)
		break
	case 4:
		d.TenField(qi, zipNum, latestNum, newOrOld)
		break
	case 5:
		d.TenFieldWithPredicate(qi, zipNum, latestNum, newOrOld)
		break
	default:
		d.FiveField1(qi, zipNum, latestNum, newOrOld)
		break
	}

	//fmt.Printf("number:\t%d\n", index)
	index++
}
