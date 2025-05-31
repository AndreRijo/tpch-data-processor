package tpch

import (
	"fmt"
	"strconv"
	"time"
)

// TODO: Migrate some of tpchClient.go variables to use these ones instead.
// Contains some basic variables. Users are recommended to clean unecessary variables to save memory
type TpchData struct {
	RawTables [][][]string //Base, unprocessed, tables
	ToRead    [][]int8     //Fields of each table that should be read
	Keys      [][]int      //Fields that compose the primary key of each table
	Headers   [][]string   //The name of each field in each table

	ReadChan, ProcChan chan int

	Tables *Tables
	TpchConfigs
}

type TpchConfigs struct {
	Sf             float64
	DataLoc        string
	IsSingleServer bool //Necessary for when it's clients loading the data
}

type TableReadingTimes struct {
	StartTime    int64
	Header       int64
	Read         int64
	ClientTables int64
}

const (
	TableFormat, UpdFormat, Header                = "tables/%sSF/", "upds/%sSF/", "tpch_headers/tpch_headers_min.txt"
	TableExtension, UpdExtension, DeleteExtension = ".tbl", ".tbl.u", "."
)

var (
	//Contants
	TableNames   = [...]string{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}
	TableEntries = [...]int{150000, 60175, 25, 1500000, 200000, 800000, 5, 10000}
	TableParts   = [...]int{8, 16, 4, 9, 9, 5, 3, 7}
	TableUsesSF  = [...]bool{true, false, false, true, true, true, false, true}
	UpdsNames    = [...]string{"orders", "lineitem", "delete"}
	UpdEntries   = [...]int{1500, 6010, 1500}
	Buckets      = []string{"R1", "R2", "R3", "R4", "R5", "PART"}
	UpdParts     = [...]int{9, 16} //Number of fields of Order, LineItem

	TableFolder, UpdFolder, HeaderLoc string
	UpdCompleteFilename               [3]string
	times                             TableReadingTimes
)

func (data *TpchData) Initialize() {
	data.PrepVars()
	data.Tables.InitConstants(data.IsSingleServer) //TODO: Need to consider singleServer solution
	data.FixTableEntries()
}

func (data *TpchData) PrepareBaseData() (returnTimes TableReadingTimes) {
	data.ReadHeaders()
	go data.ProcessBaseData()
	data.ReadBaseData()
	data.Tables.NationsByRegion = CreateNationsByRegionTable(data.Tables.Nations, data.Tables.Regions)
	data.Tables.SortedNationsName, data.Tables.SortedNationsIds,
		data.Tables.SortedNationsIdByRegionToFull, data.Tables.SortedNationIdsByRegion = CreateSortedNations(data.Tables.Nations)
	return times
}

/*
func (data *TpchData) PrepareBaseData() {
	fmt.Println("[TPCH]Doing preparatory work...")
	data.PrepVars()
	data.FixTableEntries()
	data.ReadHeaders()
	fmt.Println("[TPCH]Reading base data...")
	data.ReadBaseData()
	fmt.Println("[TPCH]Processing base data...")
	data.ProcessBaseData()
}
*/

func (data *TpchData) PrepVars() {
	times = TableReadingTimes{StartTime: time.Now().UnixNano() / 1000000}
	scaleFactorS := strconv.FormatFloat(data.Sf, 'f', -1, 64)
	TableFolder, UpdFolder = data.DataLoc+fmt.Sprintf(TableFormat, scaleFactorS), data.DataLoc+fmt.Sprintf(UpdFormat, scaleFactorS)
	UpdCompleteFilename = [3]string{UpdFolder + UpdsNames[0] + UpdExtension, UpdFolder + UpdsNames[1] + UpdExtension,
		UpdFolder + UpdsNames[2] + DeleteExtension}
	HeaderLoc = data.DataLoc + Header
	//fmt.Printf("[TPCHProc]Common folder: %s. Table folder: %s. UpdFolder: %s. HeaderLoc: %s\n", data.DataLoc, TableFolder, UpdFolder, HeaderLoc)
	data.ReadChan, data.ProcChan = make(chan int, len(TableNames)), make(chan int, len(TableNames))
	//fmt.Printf("[TpchProc]Chan sizes: %d, %d.\n", cap(data.ReadChan), cap(data.ProcChan))
}

func (data *TpchData) FixTableEntries() {
	switch data.Sf {
	case 0.01:
		TableEntries[LINEITEM] = 60175
		//updEntries = []int{10, 37, 10}
		UpdEntries = [...]int{15, 41, 16}
	case 0.1:
		TableEntries[LINEITEM] = 600572
		//updEntries = []int{150, 592, 150}
		//updEntries = []int{150, 601, 150}
		UpdEntries = [...]int{151, 601, 150}
	case 0.2:
		TableEntries[LINEITEM] = 1800093
		UpdEntries = [...]int{300, 1164, 300} //NOTE: FAKE VALUES!
	case 0.3:
		TableEntries[LINEITEM] = 2999668
		UpdEntries = [...]int{450, 1747, 450} //NOTE: FAKE VALUES!
	case 1:
		TableEntries[LINEITEM] = 6001215
		//updEntries = []int{1500, 5822, 1500}
		//updEntries = []int{1500, 6001, 1500}
		UpdEntries = [...]int{1500, 6010, 1500}
	}
}

/*
	func (data *TpchData) ReadBaseData() {
		data.Headers, data.Keys, data.ToRead = ReadHeaders(HeaderLoc, len(TableNames))
		data.RawTables = make([][][]string, len(TableNames))
		//Force these to be read first
		data.readTable(REGION)
		data.readTable(NATION)
		data.readTable(SUPPLIER)
		data.readTable(CUSTOMER)
		data.readTable(ORDERS)
		//Order is irrelevant now
		data.readTable(LINEITEM)
		data.readTable(PARTSUPP)
		data.readTable(PART)

		data.Tables.NationsByRegion = CreateNationsByRegionTable(data.Tables.Nations, data.Tables.Regions)
	}
*/

func (data *TpchData) ReadHeaders() {
	headerStart := time.Now().UnixNano() / 1000000
	data.Headers, data.Keys, data.ToRead = ReadHeaders(HeaderLoc, len(TableNames))
	//fmt.Println("[TPCH-DL]Headers:", data.Headers)
	//fmt.Println("[TPCH-DL]Keys:", data.Keys)
	//fmt.Println("[TPCH-DL]ToRead:", data.ToRead)
	headerFinish := time.Now().UnixNano() / 1000000
	times.Header = headerFinish - headerStart
}

/*func (data *TpchData) ReadBaseData() {
	start := time.Now().UnixNano() / 1000000
	data.RawTables = make([][][]string, len(TableNames))
	//Force these to be read first
	data.readTable(REGION)
	data.readTable(NATION)
	data.readTable(CUSTOMER)
	data.readTable(SUPPLIER)
	data.readTable(ORDERS)
	//Order is irrelevant now
	data.readTable(LINEITEM)
	data.readTable(PARTSUPP)
	data.readTable(PART)

	end := time.Now()
	times.Read = end.UnixNano()/1000000 - start
	fmt.Printf("[TpchProcs]Finished reading all data files at %s. Read time: %dms\n", end.Format("15:04:05.000"), times.Read)
}*/

func (data *TpchData) ReadBaseData() {
	start := time.Now().UnixNano() / 1000000
	data.RawTables = make([][][]string, len(TableNames))
	replyChan := make(chan int, len(TableNames))
	//data.readTableHelper(REGION, replyChan)
	//data.readTableHelper(NATION, replyChan)
	data.readTable(REGION) //Force these two to be read first as they will be necessary for others
	data.readTable(NATION)
	go data.readTableHelper(CUSTOMER, replyChan)
	go data.readTableHelper(SUPPLIER, replyChan)
	go data.readTableHelper(ORDERS, replyChan)
	go data.readTableHelper(LINEITEM, replyChan)
	go data.readTableHelper(PARTSUPP, replyChan)
	go data.readTableHelper(PART, replyChan)

	//Wait for all tables to be read
	for i := 0; i < len(TableNames)-2; i++ { //REGION and NATION do not use channel.
		data.ReadChan <- <-replyChan
	}
	end := time.Now()
	times.Read = end.UnixNano()/1000000 - start
	fmt.Printf("[TpchProc]Finished reading all data files (goroutines) at %s. Read time: %dms. Time since start: %dms\n",
		end.Format("15:04:05.000"), times.Read, end.UnixNano()/1000000-times.StartTime)
}

func (data *TpchData) readTableHelper(tableN int, replyChan chan int) {
	fmt.Println("[TpchProc]Reading", TableNames[tableN], tableN)
	nEntries := TableEntries[tableN]
	if TableUsesSF[tableN] {
		nEntries = int(float64(nEntries) * data.Sf)
	}
	data.RawTables[tableN] = ReadTable(TableFolder+TableNames[tableN]+TableExtension, TableParts[tableN], nEntries, data.ToRead[tableN])
	//Read complete, can now start processing and sending it
	replyChan <- tableN
}

func (data *TpchData) readTable(tableN int) {
	fmt.Println("[TpchProc]Reading", TableNames[tableN], tableN)
	nEntries := TableEntries[tableN]
	if TableUsesSF[tableN] {
		nEntries = int(float64(nEntries) * data.Sf)
	}
	data.RawTables[tableN] = ReadTable(TableFolder+TableNames[tableN]+TableExtension, TableParts[tableN], nEntries, data.ToRead[tableN])
	//Read complete, can now start processing and sending it
	data.ReadChan <- tableN
}

/*
func (data *TpchData) ProcessBaseData() {
	data.processTable(REGION)
	data.processTable(NATION)
	data.processTable(SUPPLIER)
	data.processTable(CUSTOMER)
	data.processTable(ORDERS)
	data.processTable(LINEITEM)
	data.processTable(PARTSUPP)
	data.processTable(PART)
}
*/

// Now that lineitem processTable is optimized with goroutines, it's not worth to split this one into goroutines
// This because readtable for lineitem will always be the last one to finish: so we will process all other tables before we receive lineitems
func (data *TpchData) ProcessBaseData() {
	start := int64(0)
	timeTaken := int64(0) //TODO: Disable this one, just to know for now.
	for left := len(TableNames); left > 0; left-- {
		tableN := <-data.ReadChan
		if start == 0 {
			start = time.Now().UnixNano() / 1000000
		}
		thisTableStart := time.Now().UnixNano() / 1000000
		fmt.Println("[TpchProc]Creating", TableNames[tableN], tableN)
		data.processTable(tableN)
		timeTaken += time.Now().UnixNano()/1000000 - thisTableStart
		fmt.Printf("[TpchProc]Sending to chan that table %d is complete. Size of channel: %d\n", tableN, cap(data.ProcChan))
		if left == 1 { //Last table. We want to ensure we create the following entries before notifying the channel.
			data.Tables.NationsByRegion = CreateNationsByRegionTable(data.Tables.Nations, data.Tables.Regions)
			data.Tables.SortedNationsName, data.Tables.SortedNationsIds,
				data.Tables.SortedNationsIdByRegionToFull, data.Tables.SortedNationIdsByRegion = CreateSortedNations(data.Tables.Nations)
			data.Tables.ColorsOfPart = CreateColorsOfParts(data.Tables.Parts)
		}
		data.ProcChan <- tableN
	}
	end := time.Now()
	times.ClientTables = end.UnixNano()/1000000 - start
	fmt.Printf("[TpchProc]Finished processing all tables at %s. Took %dms. Actual time spent processing: %dms. Time since start: %dms\n",
		end.Format("15:04:05.000"), times.ClientTables, timeTaken, end.UnixNano()/1000000-times.StartTime)
}

func (data *TpchData) processTable(tableN int) {
	switch tableN {
	case CUSTOMER:
		data.Tables.CreateCustomers(data.RawTables)
	case LINEITEM:
		data.Tables.CreateLineitems(data.RawTables)
	case NATION:
		data.Tables.CreateNations(data.RawTables)
	case ORDERS:
		data.Tables.CreateOrders(data.RawTables)
	case PART:
		data.Tables.CreateParts(data.RawTables)
	case REGION:
		data.Tables.CreateRegions(data.RawTables)
	case PARTSUPP:
		data.Tables.CreatePartsupps(data.RawTables)
	case SUPPLIER:
		data.Tables.CreateSuppliers(data.RawTables)
	}
}

func (data *TpchData) CleanAll() {
	data.RawTables, data.ToRead, data.Keys, data.Headers, data.Tables = nil, nil, nil, nil, nil
}

func min(first int, second int) int {
	if first < second {
		return first
	}
	return second
}

/*
func getEntryORMapUpd(headers []string, primKeys []int, table []string, read []int8) (objKey string, entriesUpd *crdt.MapAddAll) {
	entries := make(map[string]crdt.Element)
	for _, tableI := range read {
		entries[headers[tableI]] = crdt.Element(table[tableI])
	}

	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(table[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], &crdt.MapAddAll{Values: entries}
}
*/
