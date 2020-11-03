package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap/go-tpc/pkg/load"
	"github.com/pingcap/go-tpc/pkg/workload"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type ParquetWorkLoader struct {
	db  *sql.DB
	cfg *Config

	// tables is a set contains which table needs
	// to be generated when preparing csv data.
	tables map[string]bool

	createTableWg sync.WaitGroup
	initLoadTime  int64

	ddlManager *ddlManager
}

// NewCSVWorkloader creates the tpc-c workloader to generate CSV files
func NewParquetWorkLoader(db *sql.DB, cfg *Config) (*ParquetWorkLoader, error) {
	if cfg.Parts > cfg.Warehouses {
		panic(fmt.Errorf("number warehouses %d must >= partition %d", cfg.Warehouses, cfg.Parts))
	}

	w := &ParquetWorkLoader{
		db:           db,
		cfg:          cfg,
		initLoadTime: time.Now().UnixNano() / 1000,
		tables:       make(map[string]bool),
		ddlManager:   newDDLManager(cfg.Parts, cfg.UseFK),
	}

	var val bool
	if len(cfg.SpecifiedTables) == 0 {
		val = true
	}
	for _, table := range tables {
		w.tables[table] = val
	}

	if _, err := os.Stat(w.cfg.OutputDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(w.cfg.OutputDir, os.ModePerm); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	if len(cfg.SpecifiedTables) > 0 {
		for _, t := range strings.Split(cfg.SpecifiedTables, ",") {
			if _, ok := w.tables[t]; !ok {
				return nil, fmt.Errorf("\nTable %s is not supported.\nSupported tables: item, customer, district, "+
					"orders, new_order, order_line, history, warehouse, stock.", t)
			}
			w.tables[t] = true
		}
	}

	if !w.tables[tableOrders] && w.tables[tableOrderLine] {
		return nil, fmt.Errorf("\nTable orders must be specified if you want to generate table order_line.")
	}
	if w.db != nil {
		w.createTableWg.Add(cfg.Threads)
	}

	return w, nil
}

func (c *ParquetWorkLoader) Name() string {
	return "tpcc-parquet"
}

func (c *ParquetWorkLoader) InitThread(ctx context.Context, threadID int) context.Context {
	s := &tpccState{
		TpcState: workload.NewTpcState(ctx, c.db),
	}

	s.loaders = make(map[string]load.BatchLoader)
	for k, v := range c.tables {
		// table item only created at thread 0
		var obj interface{}
		switch k {
		case "item":
			obj = &Item{}
		case tableWareHouse:
			obj = &Warehouse{}
		case "stock":
			obj = &Stock{}
		case "district":
			obj = &District{}
		case "customer":
			obj = &Customer{}
		case "history":
			obj = &History{}
		case "orders":
			obj = &Orders{}
		case "new_order":
			obj = &NewOrder{}
		case "order_line":
			obj = &OrderLine{}
		default:
			panic(fmt.Sprintf("invalid table '%s'", k))
		}
		if v && !(k == "item" && threadID != 0) {
			file := path.Join(c.cfg.OutputDir, fmt.Sprintf("%s.%s.%d.parquet", c.DBName(), k, threadID))
			w, err := load.NewParquetBatchLoader(file, obj, k)
			if err != nil {
				panic(err)
			}
			s.loaders[k] = w
		}
	}

	ctx = context.WithValue(ctx, stateKey, s)
	return ctx
}

func (c *ParquetWorkLoader) CleanupThread(ctx context.Context, _ int) {
	s := getTPCCState(ctx)
	if s.Conn != nil {
		s.Conn.Close()
	}
	for k, _ := range s.loaders {
		s.loaders[k].Flush(ctx)
		s.loaders[k].Close(ctx)
	}
}

func (c *ParquetWorkLoader) Prepare(ctx context.Context, threadID int) error {
	if c.db != nil {
		if threadID == 0 {
			if err := c.ddlManager.createTables(ctx); err != nil {
				return err
			}
		}
		c.createTableWg.Done()
		c.createTableWg.Wait()
	}

	return prepareWorkload(ctx, c, c.cfg.Threads, c.cfg.Warehouses, threadID)
}

// CSV type doesn't support CheckPrepare
func (c *ParquetWorkLoader) CheckPrepare(_ context.Context, _ int) error {
	return nil
}

// CSV type doesn't support Run
func (c *ParquetWorkLoader) Run(_ context.Context, _ int) error {

	return nil
}

// CSV type doesn't support Cleanup
func (c *ParquetWorkLoader) Cleanup(_ context.Context, _ int) error {
	return nil
}

// CSV type doesn't support Check
func (c *ParquetWorkLoader) Check(_ context.Context, _ int) error {
	return nil
}

// OutputStats just do nothing
func (c *ParquetWorkLoader) OutputStats(_ bool) {}

func (c *ParquetWorkLoader) DBName() string {
	return c.cfg.DBName
}

type Item struct {
	I_id    int32  `parquet:"name=i_id, type=INT32"`
	I_im_id int32  `parquet:"name=i_im_id, type=INT32"`
	I_name  string `parquet:"name=i_name, type=UTF8, encoding=PLAIN_DICTIONARY"`
	I_price int32  `parquet:"name=i_price, type=DECIMAL, scale=2, precision=5, basetype=INT32"`
	I_data  string `parquet:"name=i_data, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

func (c *ParquetWorkLoader) loadItem(ctx context.Context) error {
	if !c.tables[tableItem] {
		return nil
	}

	fmt.Printf("load to item\n")
	s := getTPCCState(ctx)
	l := s.loaders[tableItem]

	for i := 0; i < maxItems; i++ {
		s.Buf.Reset()

		iImID := randInt(s.R, 1, 10000)
		iPrice := int32(randInt(s.R, 100, 10000))
		iName := randChars(s.R, s.Buf, 14, 24)
		iData := randOriginalString(s.R, s.Buf)

		v := &Item{int32(i + 1), int32(iImID), iName, iPrice, iData}
		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return l.Flush(ctx)
}

type Warehouse struct {
	Id      int32  `parquet:"name=w_id, type=INT32"`
	Name    string `parquet:"name=w_name, type=UTF8"`
	Street1 string `parquet:"name=w_street_1, type=UTF8"`
	Street2 string `parquet:"name=w_street_2, type=UTF8"`
	City    string `parquet:"name=w_city, type=UTF8"`
	State   string `parquet:"name=w_state, type=UTF8"`
	Zip     string `parquet:"name=w_zip, type=UTF8"`
	Tax     int32  `parquet:"name=w_tax, type=DECIMAL, scale=4, precision=4, basetype=INT32"`
	Ytd     int64  `parquet:"name=w_ytd, type=DECIMAL, scale=2, precision=12, basetype=INT64"`
}

func (c *ParquetWorkLoader) loadWarehouse(ctx context.Context, warehouse int) error {
	if !c.tables[tableWareHouse] {
		return nil
	}
	s := getTPCCState(ctx)
	l := s.loaders[tableWareHouse]

	wName := randChars(s.R, s.Buf, 6, 10)
	wStree1 := randChars(s.R, s.Buf, 10, 20)
	wStree2 := randChars(s.R, s.Buf, 10, 20)
	wCity := randChars(s.R, s.Buf, 10, 20)
	wState := randState(s.R, s.Buf)
	wZip := randZip(s.R, s.Buf)
	wTax := int32(randTax(s.R) * 10000)
	wYtd := int64(300000.00 * 100)

	v := &Warehouse{int32(warehouse), wName, wStree1, wStree2, wCity, wState, wZip, wTax, wYtd}
	if err := l.InsertValue(ctx, v); err != nil {
		return err
	}
	return nil
	//return l.Flush(ctx)
}

type Stock struct {
	IId       int32  `parquet:"name=s_i_id, type=INT32"`
	WId       int32  `parquet:"name=s_w_id, type=INT32"`
	Quantity  int32  `parquet:"name=s_quantity, type=INT32"`
	Dist01    string `parquet:"name=s_dist_01, type=UTF8"`
	Dist02    string `parquet:"name=s_dist_02, type=UTF8"`
	Dist03    string `parquet:"name=s_dist_03, type=UTF8"`
	Dist04    string `parquet:"name=s_dist_04, type=UTF8"`
	Dist05    string `parquet:"name=s_dist_05, type=UTF8"`
	Dist06    string `parquet:"name=s_dist_06, type=UTF8"`
	Dist07    string `parquet:"name=s_dist_07, type=UTF8"`
	Dist08    string `parquet:"name=s_dist_08, type=UTF8"`
	Dist09    string `parquet:"name=s_dist_09, type=UTF8"`
	Dist10    string `parquet:"name=s_dist_10, type=UTF8"`
	Ytd       int32  `parquet:"name=s_ytd, type=INT32"`
	OrderCnt  int32  `parquet:"name=s_order_cnt, type=INT32"`
	RemoteCnt int32  `parquet:"name=s_remote_cnt, type=INT32"`
	Data      string `parquet:"name=s_data, type=UTF8"`
}

func (c *ParquetWorkLoader) loadStock(ctx context.Context, warehouse int) error {
	if !c.tables[tableStock] {
		return nil
	}
	//fmt.Printf("load to stock in warehouse %d\n", warehouse)

	s := getTPCCState(ctx)
	l := s.loaders[tableStock]

	for i := 0; i < stockPerWarehouse; i++ {
		s.Buf.Reset()

		sIID := i + 1
		sWID := warehouse
		sQuantity := randInt(s.R, 10, 100)
		sDist01 := randLetters(s.R, s.Buf, 24, 24)
		sDist02 := randLetters(s.R, s.Buf, 24, 24)
		sDist03 := randLetters(s.R, s.Buf, 24, 24)
		sDist04 := randLetters(s.R, s.Buf, 24, 24)
		sDist05 := randLetters(s.R, s.Buf, 24, 24)
		sDist06 := randLetters(s.R, s.Buf, 24, 24)
		sDist07 := randLetters(s.R, s.Buf, 24, 24)
		sDist08 := randLetters(s.R, s.Buf, 24, 24)
		sDist09 := randLetters(s.R, s.Buf, 24, 24)
		sDist10 := randLetters(s.R, s.Buf, 24, 24)
		sYtd := 0
		sOrderCnt := 0
		sRemoteCnt := 0
		sData := randOriginalString(s.R, s.Buf)

		v := &Stock{int32(sIID), int32(sWID), int32(sQuantity), sDist01, sDist02, sDist03, sDist04, sDist05, sDist06,
			sDist07, sDist08, sDist09, sDist10, int32(sYtd), int32(sOrderCnt), int32(sRemoteCnt), sData}

		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return nil
	//return l.Flush(ctx)
}

type District struct {
	Id      int32  `parquet:"name=d_id, type=INT32"`
	WId     int32  `parquet:"name=d_w_id, type=INT32"`
	Name    string `parquet:"name=d_name, type=UTF8"`
	Street1 string `parquet:"name=d_street_1, type=UTF8"`
	Street2 string `parquet:"name=d_street_2, type=UTF8"`
	City    string `parquet:"name=d_city, type=UTF8"`
	State   string `parquet:"name=d_state, type=UTF8"`
	Zip     string `parquet:"name=d_zip, type=UTF8"`
	Tax     int32  `parquet:"name=d_tax, type=DECIMAL, scale=4, precision=4, basetype=INT32"`
	Ytd     int64  `parquet:"name=d_ytd, type=DECIMAL, scale=2, precision=12, basetype=INT64"`
	NextOId int32  `parquet:"name=d_next_o_id, type=INT32"`
}

func (c *ParquetWorkLoader) loadDistrict(ctx context.Context, warehouse int) error {
	if !c.tables[tableDistrict] {
		return nil
	}
	//fmt.Printf("load to district in warehouse %d\n", warehouse)

	s := getTPCCState(ctx)
	l := s.loaders[tableDistrict]

	for i := 0; i < districtPerWarehouse; i++ {
		s.Buf.Reset()

		dID := i + 1
		dWID := warehouse
		dName := randChars(s.R, s.Buf, 6, 10)
		dStreet1 := randChars(s.R, s.Buf, 10, 20)
		dStreet2 := randChars(s.R, s.Buf, 10, 20)
		dCity := randChars(s.R, s.Buf, 10, 20)
		dState := randState(s.R, s.Buf)
		dZip := randZip(s.R, s.Buf)
		dTax := int32(randTax(s.R) * 10000)
		dYtd := 30000_00
		dNextOID := 3001

		v := &District{int32(dID), int32(dWID), dName, dStreet1, dStreet2, dCity, dState, dZip,
			dTax, int64(dYtd), int32(dNextOID)}

		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return nil
	//return l.Flush(ctx)
}

type Customer struct {
	Id          int32  `parquet:"name=c_id, type=INT32"`
	DId         int32  `parquet:"name=c_d_id, type=INT32"`
	WId         int32  `parquet:"name=c_w_id, type=INT32"`
	First       string `parquet:"name=c_first, type=UTF8, encoding=PLAIN"`
	Middle      string `parquet:"name=c_middle, type=UTF8, encoding=PLAIN"`
	Last        string `parquet:"name=c_last, type=UTF8, encoding=PLAIN"`
	Street1     string `parquet:"name=c_street_1, type=UTF8, encoding=PLAIN"`
	Street2     string `parquet:"name=c_street_2, type=UTF8, encoding=PLAIN"`
	City        string `parquet:"name=c_city, type=UTF8, encoding=PLAIN"`
	State       string `parquet:"name=c_state, type=UTF8, encoding=PLAIN"`
	Zip         string `parquet:"name=c_zip, type=UTF8, encoding=PLAIN"`
	Phone       string `parquet:"name=c_phone, type=UTF8, encoding=PLAIN"`
	Since       int64  `parquet:"name=c_since, type=TIMESTAMP_MICROS"`
	Credit      string `parquet:"name=c_credit, type=UTF8, encoding=PLAIN"`
	CreditLim   int64  `parquet:"name=c_credit_lim, type=DECIMAL, scale=2, precision=12, basetype=INT64"`
	Discount    int32  `parquet:"name=c_discount, type=DECIMAL, scale=4, precision=4, basetype=INT32"`
	Balance     int64  `parquet:"name=c_balance, type=DECIMAL, scale=2, precision=12, basetype=INT64"`
	YtdPayment  int64  `parquet:"name=c_ytd_payment, type=DECIMAL, scale=2, precision=12, basetype=INT64"`
	PaymentCnt  int32  `parquet:"name=c_payment_cnt, type=INT64"`
	DeliveryCnt int32  `parquet:"name=c_delivery_cnt, type=INT32"`
	Data        string `parquet:"name=c_data, type=UTF8, encoding=PLAIN"`
}

func (c *ParquetWorkLoader) loadCustomer(ctx context.Context, warehouse int, district int) error {
	if !c.tables[tableCustomer] {
		return nil
	}
	//fmt.Printf("load to customer in warehouse %d district %d\n", warehouse, district)

	s := getTPCCState(ctx)
	l := s.loaders[tableCustomer]

	for i := 0; i < customerPerDistrict; i++ {
		s.Buf.Reset()

		cID := i + 1
		cDID := district
		cWID := warehouse
		var cLast string
		if i < 1000 {
			cLast = randCLastSyllables(i, s.Buf)
		} else {
			cLast = randCLast(s.R, s.Buf)
		}
		cMiddle := "OE"
		cFirst := randChars(s.R, s.Buf, 8, 16)
		cStreet1 := randChars(s.R, s.Buf, 10, 20)
		cStreet2 := randChars(s.R, s.Buf, 10, 20)
		cCity := randChars(s.R, s.Buf, 10, 20)
		cState := randState(s.R, s.Buf)
		cZip := randZip(s.R, s.Buf)
		cPhone := randNumbers(s.R, s.Buf, 16, 16)
		cSince := c.initLoadTime
		cCredit := "GC"
		if s.R.Intn(10) == 0 {
			cCredit = "BC"
		}
		cCreditLim := 50000.00 * 100
		cDisCount := int32(randInt(s.R, 0, 5000))
		cBalance := int64(-10 * 10000)
		cYtdPayment := int64(10.00 * 10000)
		cPaymentCnt := 1
		cDeliveryCnt := 0
		cData := randChars(s.R, s.Buf, 300, 500)

		v := &Customer{int32(cID), int32(cDID), int32(cWID), cFirst, cMiddle, cLast, cStreet1, cStreet2, cCity, cState,
			cZip, cPhone, cSince, cCredit, int64(cCreditLim), cDisCount,
			cBalance, cYtdPayment, int32(cPaymentCnt), int32(cDeliveryCnt), cData}

		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return nil
	//return l.Flush(ctx)
}

type History struct {
	CId    int32  `parquet:"name=h_c_id, type=INT32"`
	CDId   int32  `parquet:"name=h_c_d_id, type=INT32"`
	CWId   int32  `parquet:"name=h_c_w_id, type=INT32"`
	DID    int32  `parquet:"name=h_d_id, type=INT32"`
	WID    int32  `parquet:"name=h_w_id, type=INT32"`
	Date   int64  `parquet:"name=h_date, type=TIMESTAMP_MICROS"`
	Amount int32  `parquet:"name=h_amount, type=DECIMAL, scale=2, precision=6, basetype=INT32"`
	Data   string `parquet:"name=h_data, type=UTF8"`
}

func (c *ParquetWorkLoader) loadHistory(ctx context.Context, warehouse int, district int) error {
	if !c.tables[tableHistory] {
		return nil
	}
	//fmt.Printf("load to history in warehouse %d district %d\n", warehouse, district)

	s := getTPCCState(ctx)
	l := s.loaders[tableHistory]

	// 1 customer has 1 row
	for i := 0; i < customerPerDistrict; i++ {
		s.Buf.Reset()

		hCID := i + 1
		hCDID := district
		hCWID := warehouse
		hDID := district
		hWID := warehouse
		hDate := c.initLoadTime
		hAmount := int32(10.00 * 100)
		hData := randChars(s.R, s.Buf, 12, 24)

		v := &History{int32(hCID), int32(hCDID), int32(hCWID), int32(hDID), int32(hWID),
			hDate, hAmount, hData}

		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return nil
	//return l.Flush(ctx)
}

type Orders struct {
	Id        int32 `parquet:"name=o_id, type=INT32"`
	DId       int32 `parquet:"name=o_d_id, type=INT32"`
	WId       int32 `parquet:"name=o_w_id, type=INT32"`
	CId       int32 `parquet:"name=o_c_id, type=INT32"`
	EntryD    int64 `parquet:"name=o_entry_d, type=TIMESTAMP_MICROS"`
	CarrierId int32 `parquet:"name=o_carrier_id, type=INT32"`
	OlCnt     int32 `parquet:"name=o_ol_cnt, type=INT32"`
	AllLocal  int32 `parquet:"name=o_all_local, type=INT32"`
}

func (c *ParquetWorkLoader) loadOrder(ctx context.Context, warehouse int, district int) ([]int, error) {
	if !c.tables[tableOrders] {
		return nil, nil
	}
	//fmt.Printf("load to orders in warehouse %d district %d\n", warehouse, district)

	s := getTPCCState(ctx)
	l := s.loaders[tableOrders]

	cids := rand.Perm(orderPerDistrict)
	s.R.Shuffle(len(cids), func(i, j int) {
		cids[i], cids[j] = cids[j], cids[i]
	})
	olCnts := make([]int, orderPerDistrict)
	for i := 0; i < orderPerDistrict; i++ {
		s.Buf.Reset()

		oID := i + 1
		oCID := cids[i] + 1
		oDID := district
		oWID := warehouse
		oEntryD := c.initLoadTime
		oCarrierID := randInt(s.R, 1, 10)
		oOLCnt := randInt(s.R, 5, 15)
		olCnts[i] = oOLCnt
		oAllLocal := 1
		v := &Orders{int32(oID), int32(oDID), int32(oWID), int32(oCID), oEntryD,
			int32(oCarrierID), int32(oOLCnt), int32(oAllLocal)}
		if err := l.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return olCnts, nil
	//return olCnts, l.Flush(ctx)
}

type NewOrder struct {
	Oid int32 `parquet:"name=no_o_id, type=INT32"`
	Did int32 `parquet:"name=no_d_id, type=INT32"`
	Wid int32 `parquet:"name=no_w_id, type=INT32"`
}

func (c *ParquetWorkLoader) loadNewOrder(ctx context.Context, warehouse int, district int) error {
	if !c.tables[tableNewOrder] {
		return nil
	}
	//fmt.Printf("load to new_order in warehouse %d district %d\n", warehouse, district)

	s := getTPCCState(ctx)

	l := s.loaders[tableNewOrder]

	for i := 0; i < newOrderPerDistrict; i++ {
		s.Buf.Reset()

		noOID := 2101 + i
		noDID := district
		noWID := warehouse

		v := &NewOrder{int32(noOID), int32(noDID), int32(noWID)}
		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return nil
	//return l.Flush(ctx)
}

type OrderLine struct {
	Oid       int32  `parquet:"name=ol_o_id, type=INT32"`
	Did       int32  `parquet:"name=ol_d_id, type=INT32"`
	Wid       int32  `parquet:"name=ol_w_id, type=INT32"`
	Number    int32  `parquet:"name=ol_number, type=INT32"`
	Iid       int32  `parquet:"name=ol_i_id, type=INT32"`
	SupplyWId int32  `parquet:"name=ol_supply_w_id, type=INT32"`
	DeliveryD int64  `parquet:"name=ol_delivery_d, type=TIMESTAMP_MICROS"`
	Quantity  int32  `parquet:"name=ol_quantity, type=INT32"`
	Amount    *int32 `parquet:"name=ol_amount, type=DECIMAL, scale=2, precision=6, basetype=INT32"`
	DistInfo  string `parquet:"name=ol_dist_info, type=UTF8"`
}

func (c *ParquetWorkLoader) loadOrderLine(ctx context.Context, warehouse int,
	district int, olCnts []int) error {
	if !c.tables[tableOrderLine] {
		return nil
	}
	//fmt.Printf("load to order_line in warehouse %d district %d\n", warehouse, district)

	s := getTPCCState(ctx)

	l := s.loaders[tableOrderLine]

	for i := 0; i < orderPerDistrict; i++ {
		for j := 0; j < olCnts[i]; j++ {
			s.Buf.Reset()

			olOID := i + 1
			olDID := district
			olWID := warehouse
			olNumber := j + 1
			olIID := randInt(s.R, 1, 100000)
			olSupplyWID := warehouse
			olQuantity := 5

			var olAmount *int32
			olDeliveryD := c.initLoadTime
			if olOID < 2101 {
				//olAmount = 0
			} else {
				amount := int32(randInt(s.R, 1, 999999))
				olAmount = &amount
			}
			olDistInfo := randChars(s.R, s.Buf, 24, 24)

			v := &OrderLine{int32(olOID), int32(olDID), int32(olWID), int32(olNumber), int32(olIID),
				int32(olSupplyWID), olDeliveryD, int32(olQuantity), olAmount, olDistInfo}
			if err := l.InsertValue(ctx, v); err != nil {
				return err
			}
		}
	}
	return nil
	//return l.Flush(ctx)
}
