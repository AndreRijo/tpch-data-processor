package tpch

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

//*****TABLES*****//

const (
	CUSTOMER, LINEITEM, NATION, ORDERS, PART, PARTSUPP, REGION, SUPPLIER = 0, 1, 2, 3, 4, 5, 6, 7
	PROMO                                                                = "PROMO"
	//O_ORDERKEY, O_CUSTKEY                                                = 0, 1
	C_NATIONKEY, L_SUPPKEY, L_ORDERKEY, N_REGIONKEY, O_CUSTKEY, PS_SUPPKEY, S_NATIONKEY, R_REGIONKEY = 3, 2, 0, 2, 1, 1, 3, 0
	O_ORDERDATE, C_MKTSEGMENT, L_SHIPDATE, L_EXTENDEDPRICE, L_DISCOUNT, O_SHIPPRIOTITY, O_ORDERKEY   = 4, 6, 10, 5, 6, 5, 0
	MAX_CUST_BAL, MIN_CUST_BAL                                                                       = 10000.0, -1000.0 //From the TPC-H specification, [-999.99, 9999.99]
)

//TODO: Might be wise to extend all (or at least customer/orders) with REGIONKEY for faster acessing REGIONKEY when doing queries
//Alternativelly, a map of key -> regionKey would also work just fine

//Might also be worth investigating if some of those types can't be downgraded to smaller bytesizes. Specially in lineitem
//(Note that doing so will not optimize the representation in PotionDB)

//TODO: Possible optimization for saving memory (and speed up update processing):
//When possible, represent the fields with 8 bit named constants.
//E.g., L_SHIPMODE only has 8 possible values: we can define a L_SHIPMODE type that is an alias of int8.
//And then define 8 constants for it.

//For now this will be implemented with O_ORDERPRIORITY
//Alias types
//type Priority int8

type Customer struct {
	C_CUSTKEY    int32
	C_NAME       string
	C_ADDRESS    string
	C_NATIONKEY  int8
	C_PHONE      string
	C_ACCTBAL    float64
	C_MKTSEGMENT string
	C_COMMENT    string
}

// Note: Probably many here could be runes
type LineItem struct {
	L_ORDERKEY      int32
	L_PARTKEY       int32
	L_SUPPKEY       int32
	L_LINENUMBER    int8
	L_QUANTITY      int8
	L_EXTENDEDPRICE float64
	L_DISCOUNT      float64
	L_TAX           float64
	L_RETURNFLAG    string //Could be rune
	L_LINESTATUS    string //Could be rune
	L_SHIPDATE      Date
	L_COMMITDATE    Date
	L_RECEIPTDATE   Date
	L_SHIPINSTRUCT  string
	L_SHIPMODE      string //Maybe replace this with a specific type for saving space & more efficient comparison
	L_COMMENT       string
}

type Nation struct {
	N_NATIONKEY int8
	N_NAME      string
	N_REGIONKEY int8
	N_COMMENT   string
}

type Orders struct {
	O_ORDERKEY      int32
	O_CUSTKEY       int32
	O_ORDERSTATUS   string
	O_TOTALPRICE    string
	O_ORDERDATE     Date
	O_ORDERPRIORITY string
	O_CLERK         string
	O_SHIPPRIORITY  string
	O_COMMENT       string
}

type Part struct {
	P_PARTKEY     int32
	P_NAME        string
	P_MFGR        string
	P_BRAND       string
	P_TYPE        string
	P_SIZE        string
	P_CONTAINER   string
	P_RETAILPRICE string
	P_COMMENT     string
}

type Region struct {
	R_REGIONKEY int8
	R_NAME      string
	R_COMMENT   string
}

type PartSupp struct {
	PS_PARTKEY    int32
	PS_SUPPKEY    int32
	PS_AVAILQTY   int32
	PS_SUPPLYCOST float64
	PS_COMMENT    string
}

type Supplier struct {
	S_SUPPKEY   int32
	S_NAME      string
	S_ADDRESS   string
	S_NATIONKEY int8
	S_PHONE     string //string of form: 10:99-100:999-100:999-1000:9999. Could be a single int64 or 1x int8, 3x int16 to save space.
	S_ACCTBAL   string //float64 of form: -999.99 to 9999.99
	S_COMMENT   string
}

// For Customers, Suppliers and Parts the first entry is empty in order for the ID to match the position in the array
// Orders and LineItems (on 1st position) use special positioning. Always use GetOrder() method to access them
// LineItems use double position. The first index is order (the one with special positioning), the second is the linenumber.
// PartSupps is sorted by partID. Each part has exactly 4 entries in PartSupps. Thus, id=1 is in pos 0-3, id=2 is in pos 4-7, id=3 in pos 8-11, etc.
// For two PartSupps with the same PartID, they are sorted by SupplierId.
// Nations/Regions use 0-n and so do their IDs.
type Tables struct {
	Customers         []*Customer
	LineItems         [][]*LineItem
	Nations           []*Nation
	Orders            []*Orders
	Parts             []*Part
	PartSupps         []*PartSupp
	Regions           []*Region
	Suppliers         []*Supplier
	MaxOrderLineitems int32
	//Lists used
	Segments          []string
	ReturnFlags       []string
	LineStatus        []string
	Priorities        []string //O_ORDERPRIORTY uses this
	Types             []string
	TypesToShortType  map[string]string
	ShortType         []string
	MediumType        []string
	TypesToMediumType map[string]string
	Colors            []string
	Modes             []string
	Brands            []string
	Containers        []string

	//Note: Likely not all of these are needed
	Instructions []string
	Nouns        []string
	Verbs        []string
	Adjectives   []string
	Adverbs      []string
	Prepositions []string
	Auxiliaries  []string
	Terminators  []rune
	//Index helpers
	PromoParts map[int32]struct{}
	//Used by queries to know where to download the order from
	OrdersRegion []int8
	//Stores the current order to region function.
	orderToRegionFun func(int32) int8
	orderIndexFun    func(int32) int32
	//Latest added orders and lineitems. May be nil if no upds besides the initial data loading have been done
	LastAddedOrders    []*Orders
	LastAddedLineItems [][]*LineItem
	//Pos of the last order that was deleted from the original table
	LastDeletedPos int

	//For each region, stores all its nations
	NationsByRegion [][]int8

	//To help with some queries (at the moment, Q9)
	SortedNationsName             []string
	SortedNationsIds              map[int8]int32 //NatId -> Pos in slice above
	SortedNationsIdByRegionToFull []int32        //Index: position in the slice below (with offset of region*5). Pos: in SortedNationsName
	SortedNationIdsByRegion       []map[int8]int32
	SortedReturnFlagLineStatus    []string //Q1
	ColorsOfPart                  [][]string

	//Variables as well since they're different depending if we're working on single-server or multi-server modes
	NationkeyToRegionkey func(int64) int8
	SuppkeyToRegionkey   func(int64) int8
	CustkeyToRegionkey   func(int64) int8
	Custkey32ToRegionkey func(int32) int8
	OrderkeyToRegionkey  func(int32) int8
	OrderToRegionkey     func(*Orders) int8

	NOrders int //Stores the number of orders before the tables are read from the disk. Optional, but setting this earlier allows for faster data loading.
}

//*****Auxiliary data types*****//

type Date struct {
	YEAR  int16
	MONTH int8
	DAY   int8
}

// returns true if the caller is higher than the argument
func (date *Date) IsHigherOrEqual(otherDate *Date) bool {
	if date.YEAR > otherDate.YEAR {
		return true
	}
	if date.YEAR < otherDate.YEAR {
		return false
	}
	//equal years
	if date.MONTH > otherDate.MONTH {
		return true
	}
	if date.MONTH < otherDate.MONTH {
		return false
	}
	//equal year and month
	if date.DAY > otherDate.DAY {
		return true
	}
	return true
}

func (date *Date) IsLowerOrEqual(otherDate *Date) bool {
	if date.YEAR < otherDate.YEAR {
		return true
	}
	if date.YEAR > otherDate.YEAR {
		return false
	}
	//equal years
	if date.MONTH < otherDate.MONTH {
		return true
	}
	if date.MONTH > otherDate.MONTH {
		return false
	}
	//equal year and month
	if date.DAY < otherDate.DAY {
		return true
	}
	if date.DAY > otherDate.DAY {
		return false
	}
	return true
}

func (date *Date) IsLower(otherDate *Date) bool {
	if date.YEAR < otherDate.YEAR {
		return true
	}
	if date.YEAR > otherDate.YEAR {
		return false
	}
	//equal years
	if date.MONTH < otherDate.MONTH {
		return true
	}
	if date.MONTH > otherDate.MONTH {
		return false
	}
	//equal year and month
	if date.DAY < otherDate.DAY {
		return false
	}
	if date.DAY > otherDate.DAY {
		return true
	}
	return false
}

func (date *Date) IsHigher(otherDate *Date) bool {
	if date.YEAR > otherDate.YEAR {
		return true
	}
	if date.YEAR < otherDate.YEAR {
		return false
	}
	//equal years
	if date.MONTH > otherDate.MONTH {
		return true
	}
	if date.MONTH < otherDate.MONTH {
		return false
	}
	//equal year and month
	if date.DAY > otherDate.DAY {
		return true
	}
	return false
}

func (date *Date) CalculateDate(nDaysOffset int) (newDate *Date) {
	dateT := time.Date(int(date.YEAR), time.Month(int(date.MONTH)), int(date.DAY), 0, 0, 0, 0, time.UTC)
	dateT = dateT.AddDate(0, 0, nDaysOffset)
	return &Date{YEAR: int16(dateT.Year()), MONTH: int8(dateT.Month()), DAY: int8(dateT.Day())}
}

func (date *Date) CalculateDiffDate(otherDate *Date) int {
	dateT := time.Date(int(date.YEAR), time.Month(int(date.MONTH)), int(date.DAY), 0, 0, 0, 0, time.UTC)
	otherDateT := time.Date(int(otherDate.YEAR), time.Month(int(otherDate.MONTH)), int(otherDate.DAY), 0, 0, 0, 0, time.UTC)
	diff := dateT.Sub(otherDateT)
	return int(diff.Hours() / 24)
}

func (date *Date) ToString() string {
	year, month, day := strconv.Itoa(int(date.YEAR)), strconv.Itoa(int(date.MONTH)), strconv.Itoa(int(date.DAY))
	//Add 0 if month or day <= 9
	if len(month) == 1 {
		month = "0" + month
	}
	if len(day) == 1 {
		day = "0" + day
	}
	return year + "-" + month + "-" + day
}

func (tab *Tables) GetShallowCopy() (copyTables *Tables) {
	return &Tables{
		Customers:                     tab.Customers,
		LineItems:                     tab.LineItems,
		Nations:                       tab.Nations,
		Orders:                        tab.Orders,
		Parts:                         tab.Parts,
		PartSupps:                     tab.PartSupps,
		Regions:                       tab.Regions,
		Suppliers:                     tab.Suppliers,
		MaxOrderLineitems:             tab.MaxOrderLineitems,
		Segments:                      tab.Segments,
		ReturnFlags:                   tab.ReturnFlags,
		LineStatus:                    tab.LineStatus,
		Types:                         tab.Types,
		TypesToShortType:              tab.TypesToShortType,
		ShortType:                     tab.ShortType,
		MediumType:                    tab.MediumType,
		Colors:                        tab.Colors,
		Modes:                         tab.Modes,
		Priorities:                    tab.Priorities,
		Brands:                        tab.Brands,
		Containers:                    tab.Containers,
		PromoParts:                    tab.PromoParts,
		orderToRegionFun:              tab.orderToRegionFun,
		orderIndexFun:                 tab.orderIndexFun,
		LastDeletedPos:                tab.LastDeletedPos,
		LastAddedOrders:               tab.LastAddedOrders,
		LastAddedLineItems:            tab.LastAddedLineItems,
		NationkeyToRegionkey:          tab.NationkeyToRegionkey,
		SuppkeyToRegionkey:            tab.SuppkeyToRegionkey,
		CustkeyToRegionkey:            tab.CustkeyToRegionkey,
		Custkey32ToRegionkey:          tab.Custkey32ToRegionkey,
		OrderkeyToRegionkey:           tab.OrderkeyToRegionkey,
		OrderToRegionkey:              tab.OrderToRegionkey,
		NationsByRegion:               tab.NationsByRegion,
		SortedNationsName:             tab.SortedNationsName,
		SortedNationsIds:              tab.SortedNationsIds,
		SortedNationsIdByRegionToFull: tab.SortedNationsIdByRegionToFull,
		SortedNationIdsByRegion:       tab.SortedNationIdsByRegion,
		SortedReturnFlagLineStatus:    tab.SortedReturnFlagLineStatus,
	}
}

func CreateClientTables(rawData [][][]string, singleServer bool) (tables *Tables) {
	startTime := time.Now().UnixNano() / 1000000
	lineItems, maxOrderLineitems := createLineitemTable(rawData[1], len(rawData[3]))
	parts, promoParts := createPartTable(rawData[4])
	tables = &Tables{
		Customers:         createCustomerTable(rawData[0]),
		LineItems:         lineItems,
		Nations:           createNationTable(rawData[2]),
		Orders:            createOrdersTable(rawData[3]),
		Parts:             parts,
		PartSupps:         createPartsuppTable(rawData[5]),
		Regions:           createRegionTable(rawData[6]),
		Suppliers:         createSupplierTable(rawData[7]),
		MaxOrderLineitems: maxOrderLineitems,
		Segments:          createSegmentsList(),
		ReturnFlags:       createReturnflagList(),
		LineStatus:        createLineStatusList(),
		Colors:            createColorsList(),
		Modes:             createModesList(),
		Priorities:        createPrioritiesList(),
		Brands:            createBrandsList(),
		Containers:        createContainersList(),
		PromoParts:        promoParts,
		ColorsOfPart:      CreateColorsOfParts(parts),
	}
	tables.Types, tables.ShortType, tables.MediumType, tables.TypesToShortType, tables.TypesToMediumType = createTypesMap()
	if !singleServer {
		tables.NationkeyToRegionkey = tables.nationkeyToRegionkey
		tables.SuppkeyToRegionkey = tables.suppkeyToRegionkey
		tables.CustkeyToRegionkey = tables.custkeyToRegionkey
		tables.Custkey32ToRegionkey = tables.custkey32ToRegionkey
		tables.OrderkeyToRegionkey = tables.orderkeyToRegionkey
		tables.OrderToRegionkey = tables.orderToRegionkey
	} else {
		tables.NationkeyToRegionkey = tables.singleServerToRegionkey
		tables.SuppkeyToRegionkey = tables.singleServerToRegionkey
		tables.CustkeyToRegionkey = tables.singleServerToRegionkey
		tables.Custkey32ToRegionkey = tables.singleServer32ToRegionkey
		tables.OrderkeyToRegionkey = tables.singleServer32ToRegionkey
		tables.OrderToRegionkey = tables.singleServerOrderToRegionkey
	}
	//tables.PromoParts = calculatePromoParts(tables.Parts)
	tables.orderToRegionFun = tables.orderkeyToRegionkeyMultiple
	tables.orderIndexFun = tables.getFullOrderIndex
	tables.LastDeletedPos = 1
	tables.NationsByRegion = CreateNationsByRegionTable(tables.Nations, tables.Regions)
	tables.SortedNationsName, tables.SortedNationsIds, tables.SortedNationsIdByRegionToFull,
		tables.SortedNationIdsByRegion = CreateSortedNations(tables.Nations)
	tables.SortedReturnFlagLineStatus = CreateSortedReturnFlagLineStatus()
	endTime := time.Now().UnixNano() / 1000000
	fmt.Println("Time taken to process tables:", endTime-startTime, "ms")
	return
}

func CreateNationsByRegionTable(nations []*Nation, regions []*Region) (result [][]int8) {
	result = make([][]int8, len(regions))
	//There's few nations so append is okay
	for _, nation := range nations {
		result[nation.N_REGIONKEY] = append(result[nation.N_REGIONKEY], nation.N_NATIONKEY)
	}
	return
}

func (tab *Tables) InitConstants(singleServer bool) {
	tab.Segments, tab.ReturnFlags = createSegmentsList(), createReturnflagList()
	tab.LineStatus, tab.Colors = createLineStatusList(), createColorsList()
	tab.Modes, tab.Priorities = createModesList(), createPrioritiesList()
	tab.Brands, tab.Containers = createBrandsList(), createContainersList()
	tab.Types, tab.ShortType, tab.MediumType, tab.TypesToShortType, tab.TypesToMediumType = createTypesMap()
	tab.SortedNationsName, tab.SortedNationsIds,
		tab.SortedNationsIdByRegionToFull, tab.SortedNationIdsByRegion = CreateSortedNations(tab.Nations)
	tab.SortedReturnFlagLineStatus = CreateSortedReturnFlagLineStatus()
	tab.orderToRegionFun = tab.orderkeyToRegionkeyMultiple
	tab.orderIndexFun = tab.getFullOrderIndex
	tab.LastDeletedPos = 1
	if !singleServer {
		tab.NationkeyToRegionkey = tab.nationkeyToRegionkey
		tab.SuppkeyToRegionkey = tab.suppkeyToRegionkey
		tab.CustkeyToRegionkey = tab.custkeyToRegionkey
		tab.Custkey32ToRegionkey = tab.custkey32ToRegionkey
		tab.OrderkeyToRegionkey = tab.orderkeyToRegionkey
		tab.OrderToRegionkey = tab.orderToRegionkey
	} else {
		tab.NationkeyToRegionkey = tab.singleServerToRegionkey
		tab.SuppkeyToRegionkey = tab.singleServerToRegionkey
		tab.CustkeyToRegionkey = tab.singleServerToRegionkey
		tab.Custkey32ToRegionkey = tab.singleServer32ToRegionkey
		tab.OrderkeyToRegionkey = tab.singleServer32ToRegionkey
		tab.OrderToRegionkey = tab.singleServerOrderToRegionkey
	}
}

func (tab *Tables) FillOrdersToRegion(updOrders [][]string) {
	//Note: processing updOrders could be more efficient if we assume the IDs are ordered (i.e., we could avoid having to do ParseInt of orderKey))
	//Total orders is *4 the initial size, but we need to remove the "+1" existent in Orders due to the first entry being empty
	tab.OrdersRegion = make([]int8, (len(tab.Orders)-1)*4+1)
	for _, order := range tab.Orders[1:] {
		tab.OrdersRegion[order.O_ORDERKEY] = tab.Nations[tab.Customers[order.O_CUSTKEY].C_NATIONKEY].N_REGIONKEY
	}
	var custKey int64
	var orderKey int64
	for _, updOrder := range updOrders {
		custKey, _ = strconv.ParseInt(updOrder[O_CUSTKEY], 10, 32)
		orderKey, _ = strconv.ParseInt(updOrder[O_ORDERKEY], 10, 32)
		tab.OrdersRegion[orderKey] = tab.Nations[tab.Customers[custKey].C_NATIONKEY].N_REGIONKEY
	}
}

func (tab *Tables) SetOrderIndexFunToUpdates() {
	tab.orderIndexFun = tab.getUpdateOrderIndex
}

func createCustomerTable(cTable [][]string) (customers []*Customer) {
	//Customers IDs are 1 -> n, so we reserve an extra empty space at start
	//fmt.Println("Creating customer table")
	customers = make([]*Customer, len(cTable)+1)
	var nationKey int64
	var acctBal float64
	for i, entry := range cTable {
		nationKey, _ = strconv.ParseInt(entry[3], 10, 8)
		acctBal, _ = strconv.ParseFloat(entry[5], 64)
		customers[i+1] = &Customer{
			C_CUSTKEY:    int32(i + 1),
			C_NAME:       entry[1],
			C_ADDRESS:    entry[2],
			C_NATIONKEY:  int8(nationKey),
			C_PHONE:      entry[4],
			C_ACCTBAL:    acctBal,
			C_MKTSEGMENT: entry[6],
			C_COMMENT:    entry[7],
		}
	}
	return
}

func createLineitemTableRoutines(liTable [][]string, nOrders int) (lineItems [][]*LineItem, maxLineItem int32) {
	//fmt.Println("Creating lineItem table with size", nOrders)
	start := time.Now().UnixNano()
	lineItems = make([][]*LineItem, nOrders)
	maxLineItem = 8

	nRoutines := 24
	slicePerRoutine := len(liTable) / nRoutines
	channels := make([]chan [][]*LineItem, nRoutines)
	for i := 0; i < nRoutines; i++ {
		channels[i] = make(chan [][]*LineItem, 1)
	}
	ranges := make([]int, nRoutines) //Need to find where a given order "ends"
	ranges[0] = 0                    //For clarity
	currOffset := slicePerRoutine
	currOrderKey := liTable[currOffset][0]

	for i := 1; i < nRoutines; i++ {
		for j := currOffset + 1; j < len(liTable); j++ {
			if currOrderKey != liTable[j][0] {
				ranges[i] = j
				currOffset = j + slicePerRoutine
				if i < nRoutines-1 {
					currOrderKey = liTable[currOffset][0]
				}
				break
			}
		}
		go helperCreateLineitemTable(liTable[ranges[i-1]:ranges[i]], maxLineItem, channels[i-1])
	}
	go helperCreateLineitemTable(liTable[ranges[len(ranges)-1]:], maxLineItem, channels[len(ranges)-1])

	offset := 0
	for _, channel := range channels { //Ensures replies are received by order
		toCopy := <-channel
		offset += copy(lineItems[offset:], toCopy)
		//offset += len(toCopy)
	}

	end := time.Now()
	fmt.Printf("[Tables]Took %d ms to create LineItem processed table, at %s (last index: %d).\n", (end.UnixNano()-start)/1000000, time.Now().Format("15:04:05.000"), offset)
	return
}

func helperCreateLineitemTable(liTable [][]string, maxLineItem int32, replyChan chan [][]*LineItem) {
	lineItems := make([][]*LineItem, len(liTable)/3) //Should be safe.
	bufItems := make([]*LineItem, maxLineItem+1)
	var newLine []*LineItem

	var partKey, orderKey, suppKey, lineNumber, quantity int64
	var convLineNumber int8
	var convOrderKey int32
	var extendedPrice, discount, tax float64
	bufI, bufOrder := 0, 0
	tmpOrderID, _ := strconv.ParseInt(liTable[0][0], 10, 32)
	currOrderID := int32(tmpOrderID)
	for _, entry := range liTable {
		//Create lineitem
		orderKey, _ = strconv.ParseInt(entry[0], 10, 32)
		partKey, _ = strconv.ParseInt(entry[1], 10, 32)
		suppKey, _ = strconv.ParseInt(entry[2], 10, 32)
		lineNumber, _ = strconv.ParseInt(entry[3], 10, 8)
		quantity, _ = strconv.ParseInt(entry[4], 10, 8)
		convLineNumber, convOrderKey = int8(lineNumber), int32(orderKey)
		extendedPrice, _ = strconv.ParseFloat(entry[5], 64)
		discount, _ = strconv.ParseFloat(entry[6], 64)
		tax, _ = strconv.ParseFloat(entry[7], 64)

		bufItems[bufI] = &LineItem{
			L_ORDERKEY:      convOrderKey,
			L_PARTKEY:       int32(partKey),
			L_SUPPKEY:       int32(suppKey),
			L_LINENUMBER:    convLineNumber,
			L_QUANTITY:      int8(quantity),
			L_EXTENDEDPRICE: extendedPrice,
			L_DISCOUNT:      discount,
			L_TAX:           tax,
			L_RETURNFLAG:    entry[8],
			L_LINESTATUS:    entry[9],
			L_SHIPDATE:      createDate(entry[10]),
			L_COMMITDATE:    createDate(entry[11]),
			L_RECEIPTDATE:   createDate(entry[12]),
			L_SHIPINSTRUCT:  entry[13],
			L_SHIPMODE:      entry[14],
			L_COMMENT:       entry[15],
		}

		//Check if it belongs to a new order
		if convOrderKey != currOrderID {
			//Add everything in the buffer apart from the new one to the table
			newLine = make([]*LineItem, bufI)
			for k, item := range bufItems[:bufI] {
				newLine[k] = item
			}
			lineItems[bufOrder] = newLine
			bufOrder++
			bufItems[0] = bufItems[bufI]
			currOrderID = convOrderKey
			bufI = 0
		}

		bufI++
		//fmt.Println(orderKey)
	}

	//fmt.Println("Last order for lineitemTable: ", bufItems[bufI-1])
	//fmt.Println("Last order already in table:", lineItems[bufOrder-1][len(lineItems[bufOrder-1])-1])
	//fmt.Println(currOrderID, bufOrder)
	//Last order
	//fmt.Printf("Last orderID: %s, last orderID already in lineItems: %d, trying to add: %d\n", liTable[len(liTable)-1][0], lineItems[bufOrder-1][0].L_ORDERKEY, currOrderID)
	newLine = make([]*LineItem, bufI)
	for k, item := range bufItems[:bufI] {
		newLine[k] = item
	}
	lineItems[bufOrder] = newLine
	lineItems = lineItems[:bufOrder+1]
	replyChan <- lineItems
	close(replyChan)
}

func createLineitemTable(liTable [][]string, nOrders int) (lineItems [][]*LineItem, maxLineItem int32) {
	//fmt.Println("Creating lineItem table with size", nOrders)
	start := time.Now().UnixNano()
	maxLineItem = 8

	lineItems = make([][]*LineItem, nOrders)
	bufItems := make([]*LineItem, maxLineItem+1)
	var newLine []*LineItem

	var partKey, orderKey, suppKey, lineNumber, quantity int64
	var convLineNumber int8
	var convOrderKey int32
	var extendedPrice, discount, tax float64
	bufI, bufOrder := 0, 0
	tmpOrderID, _ := strconv.ParseInt(liTable[0][0], 10, 32)
	currOrderID := int32(tmpOrderID)
	for _, entry := range liTable {
		//Create lineitem
		orderKey, _ = strconv.ParseInt(entry[0], 10, 32)
		partKey, _ = strconv.ParseInt(entry[1], 10, 32)
		suppKey, _ = strconv.ParseInt(entry[2], 10, 32)
		lineNumber, _ = strconv.ParseInt(entry[3], 10, 8)
		quantity, _ = strconv.ParseInt(entry[4], 10, 8)
		convLineNumber, convOrderKey = int8(lineNumber), int32(orderKey)
		extendedPrice, _ = strconv.ParseFloat(entry[5], 64)
		discount, _ = strconv.ParseFloat(entry[6], 64)
		tax, _ = strconv.ParseFloat(entry[7], 64)

		bufItems[bufI] = &LineItem{
			L_ORDERKEY:      convOrderKey,
			L_PARTKEY:       int32(partKey),
			L_SUPPKEY:       int32(suppKey),
			L_LINENUMBER:    convLineNumber,
			L_QUANTITY:      int8(quantity),
			L_EXTENDEDPRICE: extendedPrice,
			L_DISCOUNT:      discount,
			L_TAX:           tax,
			L_RETURNFLAG:    entry[8],
			L_LINESTATUS:    entry[9],
			L_SHIPDATE:      createDate(entry[10]),
			L_COMMITDATE:    createDate(entry[11]),
			L_RECEIPTDATE:   createDate(entry[12]),
			L_SHIPINSTRUCT:  entry[13],
			L_SHIPMODE:      entry[14],
			L_COMMENT:       entry[15],
		}

		//Check if it belongs to a new order
		if convOrderKey != currOrderID {
			//Add everything in the buffer apart from the new one to the table
			newLine = make([]*LineItem, bufI)
			for k, item := range bufItems[:bufI] {
				newLine[k] = item
			}
			lineItems[bufOrder] = newLine
			bufOrder++
			bufItems[0] = bufItems[bufI]
			currOrderID = convOrderKey
			bufI = 0
		}

		bufI++
		//fmt.Println(orderKey)
	}

	//fmt.Println("Last order for lineitemTable: ", bufItems[bufI-1])
	//fmt.Println("Last order already in table:", lineItems[bufOrder-1][len(lineItems[bufOrder-1])-1])
	//fmt.Println(currOrderID, bufOrder)
	//Last order
	//fmt.Printf("Last orderID: %s, last orderID already in lineItems: %d, trying to add: %d\n", liTable[len(liTable)-1][0], lineItems[bufOrder-1][0].L_ORDERKEY, currOrderID)
	newLine = make([]*LineItem, bufI)
	for k, item := range bufItems[:bufI] {
		newLine[k] = item
	}
	lineItems[bufOrder] = newLine
	end := time.Now()
	fmt.Printf("[Tables]Took %d ms to create LineItem processed table, at %s (last index: %d).\n", (end.UnixNano()-start)/1000000, time.Now().Format("15:04:05.123"), bufOrder)
	return
}

func createNationTable(nTable [][]string) (nations []*Nation) {
	//fmt.Println("Creating nation table")
	nations = make([]*Nation, len(nTable))
	var nationKey, regionKey int64
	for i, entry := range nTable {
		nationKey, _ = strconv.ParseInt(entry[0], 10, 8)
		regionKey, _ = strconv.ParseInt(entry[2], 10, 8)
		nations[i] = &Nation{
			N_NATIONKEY: int8(nationKey),
			N_NAME:      entry[1],
			N_REGIONKEY: int8(regionKey),
			N_COMMENT:   entry[3],
		}
	}
	return
}

func createOrdersTable(oTable [][]string) (orders []*Orders) {
	//fmt.Println("Creating orders table with size", len(oTable)+1)
	orders = make([]*Orders, len(oTable)+1)
	var orderKey, customerKey int64
	for i, entry := range oTable {
		orderKey, _ = strconv.ParseInt(entry[0], 10, 32)
		customerKey, _ = strconv.ParseInt(entry[1], 10, 32)
		orders[i+1] = &Orders{
			O_ORDERKEY:      int32(orderKey),
			O_CUSTKEY:       int32(customerKey),
			O_ORDERSTATUS:   entry[2],
			O_TOTALPRICE:    entry[3],
			O_ORDERDATE:     createDate(entry[4]),
			O_ORDERPRIORITY: entry[5],
			O_CLERK:         entry[6],
			O_SHIPPRIORITY:  entry[7],
			O_COMMENT:       entry[8],
		}
	}
	if orders[1].O_ORDERKEY != 1 {
		//If it's not the initial data, hide the empty position
		orders = orders[1:]
	}
	return
}

func createPartTable(pTable [][]string) (parts []*Part, promoParts map[int32]struct{}) {
	//fmt.Println("Creating parts table")
	parts = make([]*Part, len(pTable)+1)
	var partKey int64
	for i, entry := range pTable {
		partKey, _ = strconv.ParseInt(entry[0], 10, 32)
		parts[i+1] = &Part{
			P_PARTKEY:     int32(partKey),
			P_NAME:        entry[1],
			P_MFGR:        entry[2],
			P_BRAND:       entry[3],
			P_TYPE:        entry[4],
			P_SIZE:        entry[5],
			P_CONTAINER:   entry[6],
			P_RETAILPRICE: entry[7],
			P_COMMENT:     entry[8],
		}
	}
	promoParts = calculatePromoParts(parts)
	return
}

func createPartsuppTable(psTable [][]string) (partSupps []*PartSupp) {
	//fmt.Println("Creating partsupp table")
	partSupps = make([]*PartSupp, len(psTable))
	var partKey, suppKey, availQty int64
	var supplyCost float64
	for i, entry := range psTable {
		partKey, _ = strconv.ParseInt(entry[0], 10, 32)
		suppKey, _ = strconv.ParseInt(entry[1], 10, 32)
		availQty, _ = strconv.ParseInt(entry[2], 10, 32)
		supplyCost, _ = strconv.ParseFloat(entry[3], 64)
		partSupps[i] = &PartSupp{
			PS_PARTKEY:    int32(partKey),
			PS_SUPPKEY:    int32(suppKey),
			PS_AVAILQTY:   int32(availQty),
			PS_SUPPLYCOST: supplyCost,
			PS_COMMENT:    entry[4],
		}
	}
	return
}

func createRegionTable(rTable [][]string) (regions []*Region) {
	//fmt.Println("Creating region table")
	regions = make([]*Region, len(rTable))
	var regionKey int64
	for i, entry := range rTable {
		regionKey, _ = strconv.ParseInt(entry[0], 10, 8)
		regions[i] = &Region{
			R_REGIONKEY: int8(regionKey),
			R_NAME:      entry[1],
			R_COMMENT:   entry[2],
		}
	}
	return
}

func createSupplierTable(sTable [][]string) (suppliers []*Supplier) {
	//fmt.Println("Creating supplier table")
	suppliers = make([]*Supplier, len(sTable)+1)
	var suppKey, nationKey int64
	for i, entry := range sTable {
		suppKey, _ = strconv.ParseInt(entry[0], 10, 32)
		nationKey, _ = strconv.ParseInt(entry[3], 0, 8)
		suppliers[i+1] = &Supplier{
			S_SUPPKEY:   int32(suppKey),
			S_NAME:      entry[1],
			S_ADDRESS:   entry[2],
			S_NATIONKEY: int8(nationKey),
			S_PHONE:     entry[4],
			S_ACCTBAL:   entry[5],
			S_COMMENT:   entry[6],
		}
	}
	return
}

func calculatePromoParts(parts []*Part) (inPromo map[int32]struct{}) {
	inPromo = make(map[int32]struct{})
	for _, part := range parts[1:] {
		if strings.HasPrefix(part.P_TYPE, PROMO) {
			inPromo[part.P_PARTKEY] = struct{}{}
		}
	}
	return
}

func (tab *Tables) GetOrderIndex(orderKey int32) (indexKey int32) {
	return tab.orderIndexFun(orderKey)
}

func (tab *Tables) getFullOrderIndex(orderKey int32) (indexKey int32) {
	//1 -> 7: 1 -> 7
	//9 -> 15: 1 -> 7
	//32 -> 39: 8 -> 15
	//40 -> 47: 8 -> 15
	//64 -> 71: 16 -> 23
	//72 -> 79: 16 -> 23
	return orderKey%8 + 8*(orderKey/32)
}

func (tab *Tables) getUpdateOrderIndex(orderKey int32) (indexKey int32) {
	return (orderKey%8 + 8*(orderKey/32)) % int32(len(tab.Orders))
}

// 0-3, 4-7, 8-11, 12-15, 16-19, 20-23, 24-27, 28-31, 32-35, 36-39
func (tab *Tables) GetPartSuppsOfPart(partKey int32) []*PartSupp {
	minPos := (partKey - 1) * 4
	return tab.PartSupps[minPos : minPos+4]
}

// Pre-condition: there is actually a partSupp with partKey and suppKey
func (tab *Tables) GetPartSuppOfLineitem(partKey int32, suppKey int32) *PartSupp {
	minPos := (partKey - 1) * 4
	for i := int32(0); i < 4; i++ {
		if tab.PartSupps[minPos+i].PS_SUPPKEY == suppKey {
			return tab.PartSupps[minPos+i]
		}
	}
	return tab.PartSupps[minPos]
}

/*
func GetLineitemIndex(lineitemKey int8, orderKey int32, maxLineitem int32) (indexKey int32) {
	//Same idea as in getOrderIndex but... we need to find a way to manage with multiple keys
	//Note that delete deletes a whole order so all of the lineitems of that order get deleted.
	//And new lineitems are for new orders.
	//Seems like each order may have up to 7 lineitems
	//1: 1-7
	//2: 8-14
	//3: 15-21
	//... 4: 22, 5: 29, 6: 36, 7: 43-49, 8:50-56,
	//9: 1-7
	//10: 8-14
	//..
	//32: 57-63
	//64: 113-119
	//65: 120-126
	//Offset of lineItem in an order + order offset before it loops (1-8 and 9-15 use same slots e.g)
	//+ space after loop
	//1-8 are special cases
	lineKey := int32(lineitemKey)
	if orderKey <= 8 {
		return lineKey + maxLineitem*(orderKey-1)
	}
	//return (lineKey % (maxLineitem + 1)) + ((maxLineitem * (orderKey - 1)) % (maxLineitem * 8)) + maxLineitem*8*(orderKey/32)
	return (lineKey % (maxLineitem + 1)) + ((maxLineitem * (orderKey)) % (maxLineitem * 8)) + maxLineitem*8*(orderKey/32)
}
*/

func createDate(stringDate string) (date Date) {
	yearS, monthS, dayS := stringDate[0:4], stringDate[5:7], stringDate[8:10]
	year64, _ := strconv.ParseInt(yearS, 10, 16)
	month64, _ := strconv.ParseInt(monthS, 10, 8)
	day64, _ := strconv.ParseInt(dayS, 10, 8)
	return Date{
		YEAR:  int16(year64),
		MONTH: int8(month64),
		DAY:   int8(day64),
	}
}

func createSegmentsList() []string {
	return []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}
}

func createReturnflagList() []string {
	return []string{"R", "A", "N"}
}

func createLineStatusList() []string {
	return []string{"O", "F"}
}

func createPrioritiesList() []string {
	return []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}
}

func createTypesMap() (types, shortTypes, mediumTypes []string, typesToShortType, typesToMediumType map[string]string) {
	syllable1 := []string{"STANDARD ", "SMALL ", "MEDIUM ", "LARGE ", "ECONOMY ", "PROMO "}
	syllable2 := []string{"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"}
	syllable3 := []string{" TIN", " NICKEL", " BRASS", " STEEL", " COPPER"}
	types, shortTypes, mediumTypes = make([]string, len(syllable1)*len(syllable2)*len(syllable3)), make([]string, len(syllable3)), make([]string, len(syllable1)*len(syllable2))
	typesToShortType, typesToMediumType = make(map[string]string), make(map[string]string)
	ti, mi := 0, 0
	intermediateWord, completeWord := "", ""
	for _, p1 := range syllable1 {
		for _, p2 := range syllable2 {
			intermediateWord = p1 + p2
			for _, p3 := range syllable3 {
				completeWord = intermediateWord + p3
				types[ti] = completeWord
				typesToShortType[completeWord], typesToMediumType[completeWord] = p3, intermediateWord
				ti++
			}
			mediumTypes[mi] = intermediateWord
			mi++
		}
	}
	shortTypes = syllable3
	sort.Slice(mediumTypes, func(i, j int) bool { return mediumTypes[i] < mediumTypes[j] })
	return
}

func createColorsList() []string { //92 entries
	return []string{"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush",
		"brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream",
		"cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted",
		"gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki",
		"lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium",
		"metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid",
		"pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red",
		"rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate",
		"smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat",
		"white", "yellow"}
}

func createModesList() []string {
	return []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}
}

func createBrandsList() []string {
	return []string{"Brand#11", "Brand#12", "Brand#13", "Brand#14", "Brand#15", "Brand#21", "Brand#22", "Brand#23", "Brand#24", "Brand#25", "Brand#31", "Brand#32",
		"Brand#33", "Brand#34", "Brand#35", "Brand#41", "Brand#42", "Brand#43", "Brand#44", "Brand#45", "Brand#51", "Brand#52", "Brand#53", "Brand#54", "Brand#55"}
}

func createContainersList() (containers []string) {
	syllable1 := []string{"SM ", "LG ", "MED ", "JUMBO ", "WRAP "}
	syllable2 := []string{"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"}
	containers = make([]string, len(syllable1)*len(syllable2))
	i := 0
	for _, w1 := range syllable1 {
		for _, w2 := range syllable2 {
			containers[i] = w1 + w2
			i++
		}
	}
	return
}

func CreateSortedNations(nations []*Nation) (nats []string, natsIds map[int8]int32, natsRegPosInFull []int32, natsRegId []map[int8]int32) {
	nats, natsIds, natsRegPosInFull, natsRegId = make([]string, len(nations)), make(map[int8]int32, len(nations)), make([]int32, len(nations)), make([]map[int8]int32, 5)
	tmpNatIds := make([]int8, len(nations))
	tmpNatRegIds := make([][]int8, 5)
	tmpNatsRegNat := make([][]string, 5)
	/*
		//var currRegNats []string
		for i, regNatsIds := range regionNations {
			var currRegNats []string //= make([]string, len(regNatsIds))
			for _, natId := range regNatsIds {
				//currRegNats[j] = nations[natId].N_NAME
				currRegNats = append(currRegNats, nations[natId].N_NAME)
			}
			sort.Slice(currRegNats, func(i, j int) bool { return currRegNats[i] < currRegNats[j] })
			natsByRegion[i] = currRegNats
		}
	*/
	for i, nat := range nations {
		nats[i], tmpNatIds[i] = nat.N_NAME, nat.N_NATIONKEY
		tmpNatRegIds[nat.N_REGIONKEY] = append(tmpNatRegIds[nat.N_REGIONKEY], nat.N_NATIONKEY)
		tmpNatsRegNat[nat.N_REGIONKEY] = append(tmpNatsRegNat[nat.N_REGIONKEY], nat.N_NAME)
	}
	//fmt.Println("[TPCHTABLES]CreateSortedNations, nations:", nations)
	sort.Slice(tmpNatIds, func(i, j int) bool { return nats[i] < nats[j] }) //Sorts IDs by nation name
	sort.Slice(nats, func(i, j int) bool { return nats[i] < nats[j] })
	for i, natId := range tmpNatIds {
		natsIds[natId] = int32(i)
	}
	for k, regSlice := range tmpNatsRegNat {
		sort.Slice(tmpNatRegIds[k], func(i, j int) bool { return regSlice[i] < regSlice[j] })
		sort.Slice(regSlice, func(i, j int) bool { return regSlice[i] < regSlice[j] })
		natsRegId[k] = make(map[int8]int32, len(tmpNatRegIds[k]))
		for i, natId := range tmpNatRegIds[k] {
			natsRegId[k][natId] = int32(i)
			natsRegPosInFull[k*len(tmpNatRegIds)+i] = natsIds[natId] //Assign position in full sorted nation array
		}
	}
	return
}

func CreateSortedReturnFlagLineStatus() []string {
	return []string{"AF", "NF", "NO", "RF"} //Order according to alphabetic order
}
func CreateColorsOfParts(parts []*Part) (colorsOfPart [][]string) {
	colorsOfPart = make([][]string, len(parts))
	nRoutines := 5
	partsPerRoutine, endChan := (len(parts)-1)/nRoutines, make(chan bool, 5)
	for i := 0; i < nRoutines-1; i++ {
		go createColorsOfPartsHelper(parts[i*partsPerRoutine+1:(i+1)*partsPerRoutine+1], colorsOfPart[i*partsPerRoutine+1:(i+1)*partsPerRoutine+1])
	}
	go createColorsOfPartsHelper(parts[(nRoutines-1)*partsPerRoutine+1:], colorsOfPart[(nRoutines-1)*partsPerRoutine+1:])
	for i := 0; i < nRoutines; i++ {
		endChan <- true
	}
	return
}

func createColorsOfPartsHelper(subParts []*Part, subColorsOfPart [][]string) {
	for j, part := range subParts {
		subColorsOfPart[j] = strings.Split(part.P_NAME, " ")
	}
}

/*
func createTypesList() []string {
	syllable1 := []string{"STANDARD ", "SMALL ", "MEDIUM ", "LARGE ", "ECONOMY ", "PROMO "}
	syllable2 := []string{"ANODIZED ", "BURNISHED ", "PLATED ", "POLISHED ", "BRUSHED "}
	syllable3 := []string{"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"}
	types := make([]string, len(syllable1)*len(syllable2)*len(syllable3))
	i := 0
	intermediateWord := ""
	for _, p1 := range syllable1 {
		for _, p2 := range syllable2 {
			intermediateWord = p1 + p2
			for _, p3 := range syllable3 {
				types[i] = intermediateWord + p3
				i++
			}
		}
	}
	return types
}
*/

func (tab *Tables) UpdateOrderLineitems(order [][]string, lineItems [][]string) {
	//Just call createOrder and createLineitem and store them
	//fmt.Println("First orderID, first lineItem orderID, lasts:", order[0][0], lineItems[0][0], order[len(order)-1][0], lineItems[len(lineItems)-1][0])
	//fmt.Println("Sizes:", len(order), len(lineItems))
	tab.LastAddedOrders = createOrdersTable(order)
	tab.LastAddedLineItems, _ = createLineitemTable(lineItems, len(order))
}

func (tab *Tables) CreateOrder(order []string) *Orders {
	orderKey, _ := strconv.ParseInt(order[0], 10, 32)
	customerKey, _ := strconv.ParseInt(order[1], 10, 32)
	return &Orders{
		O_ORDERKEY:      int32(orderKey),
		O_CUSTKEY:       int32(customerKey),
		O_ORDERSTATUS:   order[2],
		O_TOTALPRICE:    order[3],
		O_ORDERDATE:     createDate(order[4]),
		O_ORDERPRIORITY: order[5],
		O_CLERK:         order[6],
		O_SHIPPRIORITY:  order[7],
		O_COMMENT:       order[8],
	}
}

func (tab *Tables) CreateLineitemsOfOrder(items [][]string) (lineItems []*LineItem) {
	lineItems = make([]*LineItem, len(items))
	var partKey, orderKey, suppKey, lineNumber, quantity int64
	var convLineNumber int8
	var convOrderKey int32
	var extendedPrice, discount, tax float64

	for i, item := range items {
		//Create lineitem
		orderKey, _ = strconv.ParseInt(item[0], 10, 32)
		partKey, _ = strconv.ParseInt(item[1], 10, 32)
		suppKey, _ = strconv.ParseInt(item[2], 10, 32)
		lineNumber, _ = strconv.ParseInt(item[3], 10, 8)
		quantity, _ = strconv.ParseInt(item[4], 10, 8)
		convLineNumber, convOrderKey = int8(lineNumber), int32(orderKey)
		extendedPrice, _ = strconv.ParseFloat(item[5], 32)
		discount, _ = strconv.ParseFloat(item[6], 32)
		tax, _ = strconv.ParseFloat(item[7], 32)

		lineItems[i] = &LineItem{
			L_ORDERKEY:      convOrderKey,
			L_PARTKEY:       int32(partKey),
			L_SUPPKEY:       int32(suppKey),
			L_LINENUMBER:    convLineNumber,
			L_QUANTITY:      int8(quantity),
			L_EXTENDEDPRICE: extendedPrice,
			L_DISCOUNT:      discount,
			L_TAX:           tax,
			L_RETURNFLAG:    item[8],
			L_LINESTATUS:    item[9],
			L_SHIPDATE:      createDate(item[10]),
			L_COMMITDATE:    createDate(item[11]),
			L_RECEIPTDATE:   createDate(item[12]),
			L_SHIPINSTRUCT:  item[13],
			L_SHIPMODE:      item[14],
			L_COMMENT:       item[15],
		}
	}
	return
}

func (tab *Tables) CreateCustomers(table [][][]string) {
	tab.Customers = createCustomerTable(table[CUSTOMER])
}

func (tab *Tables) CreateLineitems(table [][][]string) {
	if tab.NOrders == 0 {
		tab.NOrders = len(tab.Orders) - 1
	}
	//tmpCreateLineItemTableNoPointer(table[LINEITEM], tab.NOrders)
	//tab.LineItems, tab.MaxOrderLineitems = createLineitemTable(table[LINEITEM], tab.NOrders)
	tab.LineItems, tab.MaxOrderLineitems = createLineitemTableRoutines(table[LINEITEM], tab.NOrders)
}

func (tab *Tables) CreateNations(table [][][]string) {
	tab.Nations = createNationTable(table[NATION])
}

func (tab *Tables) CreateOrders(table [][][]string) {
	tab.Orders = createOrdersTable(table[ORDERS])
}

func (tab *Tables) CreateParts(table [][][]string) {
	tab.Parts, tab.PromoParts = createPartTable(table[PART])
}

func (tab *Tables) CreateRegions(table [][][]string) {
	tab.Regions = createRegionTable(table[REGION])
}

func (tab *Tables) CreatePartsupps(table [][][]string) {
	tab.PartSupps = createPartsuppTable(table[PARTSUPP])
}

func (tab *Tables) CreateSuppliers(table [][][]string) {
	tab.Suppliers = createSupplierTable(table[SUPPLIER])
}

func (tab *Tables) nationkeyToRegionkey(nationKey int64) int8 {
	return tab.Nations[nationKey].N_REGIONKEY
}

func (tab *Tables) suppkeyToRegionkey(suppKey int64) int8 {
	return tab.Nations[tab.Suppliers[suppKey].S_NATIONKEY].N_REGIONKEY
}

func (tab *Tables) custkeyToRegionkey(custKey int64) int8 {
	return tab.Nations[tab.Customers[custKey].C_NATIONKEY].N_REGIONKEY
}

func (tab *Tables) custkey32ToRegionkey(custKey int32) int8 {
	return tab.Nations[tab.Customers[custKey].C_NATIONKEY].N_REGIONKEY
}

func (tab *Tables) orderkeyToRegionkey(orderKey int32) int8 {
	return tab.orderToRegionFun(orderKey)
}

func (tab *Tables) orderToRegionkey(order *Orders) int8 {
	return tab.Nations[tab.Customers[order.O_CUSTKEY].C_NATIONKEY].N_REGIONKEY
}

func (tab *Tables) OrderToNationkey(order *Orders) int8 {
	return tab.Customers[order.O_CUSTKEY].C_NATIONKEY
}

func (tab *Tables) SupplierkeyToNationkey(suppKey int32) int8 {
	return tab.Suppliers[suppKey].S_NATIONKEY
}

func (tab *Tables) CustomerkeyToNationkey(custKey int32) int8 {
	return tab.Customers[custKey].C_NATIONKEY
}

func (tab *Tables) singleServerToRegionkey(key int64) int8 {
	return 0
}

func (tab *Tables) singleServer32ToRegionkey(key int32) int8 {
	return 0
}

func (tab *Tables) singleServerOrderToRegionkey(order *Orders) int8 {
	return 0
}

func (tab *Tables) orderkeyToRegionkeyMultiple(orderKey int32) int8 {
	return tab.Nations[tab.Customers[tab.Orders[tab.GetOrderIndex(orderKey)].O_CUSTKEY].C_NATIONKEY].N_REGIONKEY
}

// Uses special array instead of consulting customers and nations tab.
// TODO: On places where OrderkeyToRegionkey is referred, replace with OrderkeyToRegionkeyDirect once it's ready.
// Maybe store the function to use in a variable and replace it once appropriate?
func (tab *Tables) orderkeyToRegionkeyDirect(orderKey int32) int8 {
	return tab.OrdersRegion[orderKey]
}

func (tab *Tables) GetNationIDsOfRegion(regionKey int) []int8 {
	return tab.NationsByRegion[regionKey]
}

func (item *LineItem) ToStringSlice() (slice []string) {
	slice = make([]string, 16) //LineItem has 16 entries
	slice[0] = strconv.Itoa(int(item.L_ORDERKEY))
	slice[1] = strconv.Itoa(int(item.L_PARTKEY))
	slice[2] = strconv.Itoa(int(item.L_SUPPKEY))
	slice[3] = strconv.Itoa(int(item.L_LINENUMBER))
	slice[4] = strconv.Itoa(int(item.L_QUANTITY))
	slice[5] = strconv.FormatFloat(item.L_EXTENDEDPRICE, 'f', 2, 64)
	slice[6] = strconv.FormatFloat(item.L_DISCOUNT, 'f', 2, 64)
	slice[7] = strconv.FormatFloat(item.L_TAX, 'f', 2, 64)
	slice[8] = item.L_RETURNFLAG
	slice[9] = item.L_LINESTATUS
	slice[10] = item.L_SHIPDATE.ToString()
	slice[11] = item.L_COMMITDATE.ToString()
	slice[12] = item.L_RECEIPTDATE.ToString()
	slice[13] = item.L_SHIPINSTRUCT
	slice[14] = item.L_SHIPMODE
	slice[15] = item.L_COMMENT
	return
}

func (tab *Tables) CustSliceToRegion(obj []string) int8 {
	nationKey, _ := strconv.ParseInt(obj[C_NATIONKEY], 10, 8)
	return tab.NationkeyToRegionkey(nationKey)
}

func (tab *Tables) LineitemSliceToRegion(obj []string) []int8 {
	suppKey, _ := strconv.ParseInt(obj[L_SUPPKEY], 10, 32)
	orderKey, _ := strconv.ParseInt(obj[L_ORDERKEY], 10, 32)
	r1, r2 := tab.SuppkeyToRegionkey(suppKey), tab.OrderkeyToRegionkey(int32(orderKey))
	if r1 == r2 {
		return []int8{r1}
	}
	return []int8{r1, r2}
}

func (tab *Tables) NationSliceToRegion(obj []string) int8 {
	regionKey, _ := strconv.ParseInt(obj[N_REGIONKEY], 10, 8)
	return int8(regionKey)
}

func (tab *Tables) OrdersSliceToRegion(obj []string) int8 {
	custKey, _ := strconv.ParseInt(obj[O_CUSTKEY], 10, 32)
	return tab.CustkeyToRegionkey(custKey)
}

func (tab *Tables) PartSuppSliceToRegion(obj []string) int8 {
	suppKey, _ := strconv.ParseInt(obj[PS_SUPPKEY], 10, 32)
	return tab.SuppkeyToRegionkey(suppKey)
}

func (tab *Tables) RegionSliceToRegion(obj []string) int8 {
	regionKey, _ := strconv.ParseInt(obj[R_REGIONKEY], 10, 8)
	return int8(regionKey)
}

func (tab *Tables) SupplierSliceToRegion(obj []string) int8 {
	nationKey, _ := strconv.ParseInt(obj[S_NATIONKEY], 10, 8)
	return tab.NationkeyToRegionkey(nationKey)
}

func MonthToQuarter(month int8) int8 {
	return ((month-1)/3)*3 + 1
}

func (tab *Tables) GetOrderItemsPerSupplier(items []*LineItem) (itemsPerRegion [][]*LineItem) {
	itemsPerRegion = make([][]*LineItem, len(tab.Regions))
	var regId int8
	for _, item := range items {
		regId = tab.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
		itemsPerRegion[regId] = append(itemsPerRegion[regId], item)
	}
	return
}
