package timescaledb

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

const (
	oneMinute = 60
	oneHour   = oneMinute * 60

	timeBucketFmt    = "time_bucket('%d seconds', time)"
	nonTimeBucketFmt = "to_timestamp(((extract(epoch from time)::int)/%d)*%d)"
)

// Devops produces TimescaleDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	var hostnameClauses []string
	//if d.UseJSON {
	//	for _, s := range hostnames {
	//		hostnameClauses = append(hostnameClauses, fmt.Sprintf("tagset @> '{\"hostname\": \"%s\"}'", s))
	//	}
	//	return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE %s)", strings.Join(hostnameClauses, " OR "))
	//} else if d.UseTags {
	//	for _, s := range hostnames {
	//		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	//	}
	//	return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE hostname IN (%s))", strings.Join(hostnameClauses, ","))
	//} else {
	//	for _, s := range hostnames {
	//		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	//	}
	//	// using the OR logic here is an anti-pattern for the query planner. Doing
	//	// the IN will get translated to an ANY query and do better
	//	return fmt.Sprintf("hostname IN (%s)", strings.Join(hostnameClauses, ","))
	//}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	}
	// using the OR logic here is an anti-pattern for the query planner. Doing
	// the IN will get translated to an ANY query and do better
	return fmt.Sprintf("hostname IN (%s)", strings.Join(hostnameClauses, ","))
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getHostWhereStringAndTagString(metric string, nHosts int) (string, string) {
	hostnames, err := d.GetRandomHosts(nHosts)
	databases.PanicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames), d.getTagStringWithNames(metric, hostnames)
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

func (d *Devops) getTimeBucket(seconds int) string {
	if d.UseTimeBucket {
		return fmt.Sprintf(timeBucketFmt, seconds)
	}
	return fmt.Sprintf(nonTimeBucketFmt, seconds, seconds)
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%[1]s(%[2]s) as %[1]s_%[2]s", agg, m)
	}

	return selectClauses
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE hostname IN ('$HOSTNAME_1',...,'$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	if len(selectClauses) < 1 {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}

	sql := fmt.Sprintf(`SELECT %s AS minute,
        %s
        FROM cpu
        WHERE %s AND time >= '%s' AND time < '%s'
        GROUP BY minute ORDER BY minute ASC`,
		d.getTimeBucket(oneMinute),
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

	humanLabel := fmt.Sprintf("TimescaleDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT time_bucket('1 minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	sql := fmt.Sprintf(`SELECT %s AS minute, max(usage_user)
        FROM cpu
        WHERE time < '%s'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 5`,
		d.getTimeBucket(oneMinute),
		interval.End().Format(goTimeFmt))

	humanLabel := "TimescaleDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	selectClauses := make([]string, numMetrics)
	meanClauses := make([]string, numMetrics)
	for i, m := range metrics {
		meanClauses[i] = "mean_" + m
		selectClauses[i] = fmt.Sprintf("avg(%s) as %s", m, meanClauses[i])
	}

	hostnameField := "hostname"
	joinStr := ""
	partitionGrouping := hostnameField
	if d.UseJSON || d.UseTags {
		if d.UseJSON {
			hostnameField = "tags->>'hostname'"
		} else if d.UseTags {
			hostnameField = "tags.hostname"
		}
		joinStr = "JOIN tags ON cpu_avg.tags_id = tags.id"
		partitionGrouping = "tags_id"
	}

	sql := fmt.Sprintf(`
        WITH cpu_avg AS (
          SELECT %s as hour, %s,
          %s
          FROM cpu
          WHERE time >= '%s' AND time < '%s'
          GROUP BY 1, 2
        )
        SELECT hour, %s, %s
        FROM cpu_avg
        %s
        ORDER BY hour, %s`,
		d.getTimeBucket(oneHour),
		partitionGrouping,
		strings.Join(selectClauses, ", "),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt),
		hostnameField, strings.Join(meanClauses, ", "),
		joinStr, hostnameField)
	humanLabel := devops.GetDoubleGroupByLabel("TimescaleDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE hostname IN ('$HOSTNAME_1',...,'$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)

	metrics := devops.GetAllCPUMetrics()
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sql := fmt.Sprintf(`SELECT %s AS hour,
        %s
        FROM cpu
        WHERE %s AND time >= '%s' AND time < '%s'
        GROUP BY hour ORDER BY hour`,
		d.getTimeBucket(oneHour),
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

	humanLabel := devops.GetMaxAllLabel("TimescaleDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	var sql string
	if d.UseTags {
		sql = fmt.Sprintf("SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC")
	} else if d.UseJSON {
		sql = fmt.Sprintf("SELECT DISTINCT ON (t.tagset->>'hostname') * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.tagset->>'hostname', b.time DESC")
	} else {
		sql = fmt.Sprintf(`SELECT DISTINCT ON (hostname) * FROM cpu ORDER BY hostname, time DESC`)
	}

	humanLabel := "TimescaleDB last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("AND %s", d.getHostWhereString(nHosts))
	}

	sql := fmt.Sprintf(`SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '%s' AND time < '%s' %s`,
		interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt), hostWhereClause)

	humanLabel, err := devops.GetHighCPULabel("TimescaleDB", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// New Queries
func (d *Devops) ThreeField1(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := 0
	if zipNum < 5 {
		duration = 15
	} else {
		duration = 60
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT time_bucket('%d minute', time) as bucket,hostname,avg(usage_user),avg(usage_system),avg(usage_idle) FROM cpu WHERE %s AND time >= '%s' AND time < '%s' GROUP BY hostname,bucket ORDER BY hostname,bucket`,
		duration, hostWhereString, interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt))

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64]}#{empty}#{mean,%dm}", tagString, duration)

	humanLabel := "TimeScaleDB Three Field 1"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) ThreeField2(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := 0
	if zipNum < 5 {
		duration = 15
	} else {
		duration = 60
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT time_bucket('%d minute', time) as bucket,hostname,avg(usage_idle),avg(usage_nice),avg(usage_iowait) FROM cpu WHERE %s AND time >= '%s' AND time < '%s' GROUP BY hostname,bucket ORDER BY hostname,bucket`,
		duration, hostWhereString, interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt))

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_idle[int64],usage_nice[int64],usage_iowait[int64]}#{empty}#{mean,%dm}", tagString, duration)

	humanLabel := "TimeScaleDB Three Field 2"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) ThreeField3(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := 0
	if zipNum < 5 {
		duration = 15
	} else {
		duration = 60
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT time_bucket('%d minute', time) as bucket,hostname,avg(usage_system),avg(usage_idle),avg(usage_nice) FROM cpu WHERE %s AND time >= '%s' AND time < '%s' GROUP BY hostname,bucket ORDER BY hostname,bucket`,
		duration, hostWhereString, interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt))

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_system[int64],usage_idle[int64],usage_nice[int64]}#{empty}#{mean,%dm}", tagString, duration)

	humanLabel := "TimeScaleDB Three Field 3"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) FiveField1(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := 0
	if zipNum < 5 {
		duration = 15
	} else {
		duration = 60
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT time_bucket('%d minute', time) as bucket,hostname,avg(usage_user),avg(usage_system),avg(usage_idle),avg(usage_nice),avg(usage_iowait) FROM cpu WHERE %s AND time >= '%s' AND time < '%s' GROUP BY hostname,bucket ORDER BY hostname,bucket`,
		duration, hostWhereString, interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt))

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64]}#{empty}#{mean,%dm}", tagString, duration)

	humanLabel := "TimeScaleDB Five Field 1"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) TenField(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	duration := 0
	if zipNum < 5 {
		duration = 15
	} else {
		duration = 60
	}

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT time_bucket('%d minute', time) as bucket,hostname,avg(usage_user),avg(usage_system),avg(usage_idle),avg(usage_nice),avg(usage_iowait),avg(usage_irq),avg(usage_softirq),avg(usage_steal),avg(usage_guest),avg(usage_guest_nice) FROM cpu WHERE %s AND time >= '%s' AND time < '%s' GROUP BY hostname,bucket ORDER BY hostname,bucket`,
		duration, hostWhereString, interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt))

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64],usage_irq[int64],usage_softirq[int64],usage_steal[int64],usage_guest[int64],usage_guest_nice[int64]}#{empty}#{mean,%dm}", tagString, duration)

	humanLabel := "TimeScaleDB Ten Field "
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) TenFieldWithPredicate(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	sql := ""

	hostWhereString, tagString := d.getHostWhereStringAndTagString("cpu", common.TagNum)

	sql = fmt.Sprintf(
		`SELECT time as bucket,hostname,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM cpu WHERE %s AND time >= '%s' AND time < '%s' AND usage_user > 90 AND usage_guest > 90 ORDER BY hostname,bucket`,
		hostWhereString, interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt))

	//sql = fmt.Sprintf(
	//	`SELECT time as bucket,hostname,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM cpu WHERE %s AND time >= '%s' AND time < '%s' AND usage_user > 90 ORDER BY hostname,bucket`,
	//	hostWhereString, interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt))

	sql += ";"
	sql += fmt.Sprintf("%s#{usage_user[int64],usage_system[int64],usage_idle[int64],usage_nice[int64],usage_iowait[int64],usage_irq[int64],usage_softirq[int64],usage_steal[int64],usage_guest[int64],usage_guest_nice[int64]}#{(usage_user>90[int64])(usage_guest>90[int64])}#{empty,empty}", tagString)

	humanLabel := "TimeScaleDB Ten Field with Predicate"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

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
