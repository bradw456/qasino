package server

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	qasinotable "qasino/table"

	"github.com/gin-gonic/gin"
)

func init() {
	flag.Set("logtostderr", "true")
}

var server_running bool

func ourTestGinRouter() *gin.Engine {
	engine := gin.New()
	engine.Use(gin.LoggerWithWriter(gin.DefaultWriter, "/request"), gin.Recovery())
	return engine
}

func NewTestHttpServer(data_manager *DataManager) {
	if server_running {
		return
	}

	server_running = true

	http_rpc_router := ourTestGinRouter()

	http_rpc_router.POST("/request", func(c *gin.Context) {
		LegacyHandlePOST_request(c, data_manager)
	})

	http_rpc_router.GET("/request", func(c *gin.Context) {
		LegacyHandleGET_request(c, data_manager)
	})

	go http_rpc_router.Run(fmt.Sprintf(":%d", 15597))

	time.Sleep(1 * time.Second)

}

func HttpPublishTable(table *qasinotable.Table, t *testing.T) {
	op := RequestOp{
		Identity: "test",
		Op:       "add_table_data",
		Table:    *table,
	}

	json_string, err := json.Marshal(op)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	resp, err := http.Post("http://localhost:15597/request", "application/json", bytes.NewBuffer(json_string))

	if err != nil {
		FailFatal(t, "%s", err)
	}

	defer resp.Body.Close()

	resp_json, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	var resp_op ResponseOp

	err = json.Unmarshal(resp_json, &resp_op)

	if err != nil {
		FailFatal(t, "%s", err)
	}

	if resp_op.ResponseOp != "ok" {
		FailFatal(t, "Response Op is not ok: %s", string(resp_json))
	}

	if resp_op.Identity != "testident" {
		FailFatal(t, "Response Ident is not testident: %s", string(resp_json))
	}
}

func HttpQueryTable(reqtype string, sql string, t *testing.T) *SelectResultData {

	var err error
	var resp *http.Response

	url_base := "http://localhost:15597/request"

	switch reqtype {
	case "GET":

		url := fmt.Sprintf("%s?op=query&sql=%s", url_base, url.QueryEscape(sql))

		resp, err = http.Get(url)

	case "POST":

		op := RequestOp{
			Identity: "test",
			Op:       "query",
			Sql:      sql,
		}

		json_string, err := json.Marshal(op)
		if err != nil {
			FailFatal(t, "%s", err)
		}

		resp, err = http.Post(url_base, "application/json", bytes.NewBuffer(json_string))
	default:
		FailFatal(t, "Bad HttpQueryTable request type: %s", reqtype)
	}

	if err != nil {
		FailFatal(t, "%s", err)
	}

	defer resp.Body.Close()

	resp_json, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	var resp_op ResponseOp

	err = json.Unmarshal(resp_json, &resp_op)

	if err != nil {
		FailFatal(t, "%s: %s", err, string(resp_json))
	}

	if resp_op.ResponseOp != "result_table" {
		FailFatal(t, "Response Op is not result_table: %s", string(resp_json))
	}

	if resp_op.Identity != "testident" {
		FailFatal(t, "Response Ident is not testident: %s", string(resp_json))
	}

	if resp_op.Table == nil {
		FailFatal(t, "No table in response: %s", string(resp_json))
	}

	return resp_op.Table
}

func TestHttpReceiver(t *testing.T) {

	d := GetTestDataManager(t)

	NewTestHttpServer(d)

	table := GetDummyTable()

	HttpPublishTable(table, t)

	time.Sleep(2 * time.Second)
	d.RotateDbs()

	// Now verify the result with a query.

	sql := "SELECT * FROM dummy_test_table ORDER BY 1"

	// test POST
	result_table := HttpQueryTable("POST", sql, t)

	if CompareSelectResultAndTable(result_table, table) != 0 {
		FailFatal(t, "HttpQueryTable results from query are not expected: query ===>\n%s <=== != table ===>\n%s <===\n",
			SelectResultAsString(result_table), table.String())
	}

	// test GET
	result_table = HttpQueryTable("GET", sql, t)

	if CompareSelectResultAndTable(result_table, table) != 0 {
		FailFatal(t, "HttpQueryTable results from query are not expected: query ===>\n%s <=== != table ===>\n%s <===\n",
			SelectResultAsString(result_table), table.String())
	}

}

var chars string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func get_rand_string(N int) string {
	b := make([]byte, N)
	var j int
	for i := 0; i < N; i++ {
		j = rand.Intn(len(chars))
		b[i] = chars[j]
	}
	return string(b)
}

const (
	type_int  = 0
	type_real = 1
	type_text = 2
)

var types = []string{"int", "real", "text"}

func GetRandomTable() *qasinotable.Table {

	table := qasinotable.NewTable(fmt.Sprintf("test%s", get_rand_string(10)))

	nr_cols := rand.Intn(99) + 1
	col_types := make([]int, nr_cols)
	for i := 0; i < nr_cols; i++ {
		colname := fmt.Sprintf("col%s", get_rand_string(5))
		r := rand.Intn(3)
		table.AddColumn(colname, types[r])
		col_types[i] = r
	}

	nr_rows := rand.Intn(999) + 1
	for i := 0; i < nr_rows; i++ {
		cols := make([]interface{}, nr_cols)
		for j := 0; j < nr_cols; j++ {
			switch col_types[j] {
			case type_int:
				cols[j] = fmt.Sprintf("%d", rand.Intn(1e9))
			case type_real:
				cols[j] = fmt.Sprintf("%f", rand.Float64())
			case type_text:
				cols[j] = get_rand_string(rand.Intn(49) + 1)
			}
		}
		table.AddRow(cols...)
	}
	//fmt.Println(table.String())
	return table
}

func GetRandomTable2() *qasinotable.Table {

	table := qasinotable.NewTable(fmt.Sprintf("test%s", get_rand_string(10)))

	table.AddColumn("columnA", "int")
	table.AddColumn("columnB", "text")
	table.AddColumn("columnC", "real")

	// Add in 'order by 1' order.
	table.AddRow(1122, "The quick brown fox", 3.14159265)
	table.AddRow(987654321, "jumped over the lazy dog", 0.00001)

	return table
}

func timeit(msg string, f func()) {
	start := time.Now()
	f()
	fmt.Printf("%s took %v\n", msg, time.Since(start))
}

func TestHttpReceiverStress(t *testing.T) {

	flag.Set("v", "0") // Disable some too verbose logging.

	wg := sync.WaitGroup{}

	d := GetTestDataManager(t)

	NewTestHttpServer(d)

	wg.Add(1)
	go func() {

		// Publish 1000 tables per second for 10 seconds

		overall_start_time := time.Now()

		nr_ops_in_this_second := 0
		total_ops := 0
		start_of_ops := time.Now()

		for {

			var table *qasinotable.Table
			//timeit("random table", func() {
			table = GetRandomTable2()
			//})

			//timeit("publish table", func() {
			HttpPublishTable(table, t)
			//})

			nr_ops_in_this_second++
			total_ops++

			if nr_ops_in_this_second >= 1000 {
				elapsed := time.Since(start_of_ops)

				if elapsed > time.Second {
					time.Sleep(time.Second - elapsed)
				}

				fmt.Printf("Published %d tables in %v\n", nr_ops_in_this_second, time.Since(start_of_ops))

				nr_ops_in_this_second = 0
				start_of_ops = time.Now()
			}

			if time.Since(overall_start_time) > (10 * time.Second) {
				fmt.Printf("Published %d tables in %v\n", total_ops, time.Since(start_of_ops))
				break
			}
		}
		fmt.Printf("All done publishing tables...\n")
		wg.Done()
	}()

	wg.Add(1)
	go func() {

		// Rotate the data bases once every second.

		ticker := time.NewTicker(time.Second * 2)
		done := time.NewTimer(time.Second * 10)
	LOOP:
		for {
			select {
			case <-ticker.C:
				//timeit("rotate db", func() {
				d.RotateDbs()
				//})
			case <-done.C:
				break LOOP
			}
		}

		fmt.Printf("All done rotating...\n")

		wg.Done()

	}()

	wg.Wait()

	flag.Set("v", "1") // Disable some too verbose logging.
}
