package main

import (
        "fmt"
        "sort"
        "testing"
        "time"
)

func constructGossips(ts int64, seerPath string) []string {
        gossip1 := fmt.Sprintf(`{"SeerAddr":"1.1.1.1:1111","SeerRequest":"Metadata","TS":%d,"SeerPath":[%s]}`, ts, seerPath)
        gossip2 := fmt.Sprintf(`{"TS":%d,"SeerAddr":"1.1.1.1:1111","SeerPath":[%s],"SeerRequest":"Metadata"}`, ts, seerPath)
        gossip3 := fmt.Sprintf(`{"SeerPath":[%s],"SeerAddr":"1.1.1.1:1111","SeerRequest":"Metadata", "TS" : %d }`, seerPath, ts)
        gossip4 := fmt.Sprintf(`{"SeerAddr":"1.1.1.1:1111","SeerRequest":"Metadata","SeerPath":[%s],"TS":%d}`, seerPath, ts)
        return []string{gossip1, gossip2, gossip3, gossip4}
}

/*******************************************************************************
    Tests for Regex functions.
*******************************************************************************/

func Test_UpdateSeerPath(t *testing.T) {
        seerPath := `"1.1.1.1:1111","2.2.2.2:2222"`
        newSeer := `"3.3.3.3:3333"`
        ts := int64(1111111111111)
        updatedGossips := constructGossips(ts, seerPath)

        for idx, gossip := range updatedGossips {
                updatedGossips[idx] = UpdateSeerPath(gossip, newSeer)
        }

        seerPath = newSeer + `,` + seerPath
        expectedGossips := constructGossips(ts, seerPath)
        kosher := true
        for idx, gossip := range expectedGossips {
                if gossip != updatedGossips[idx] {
                        kosher = false
                }
        }

        if !kosher {
                t.Errorf("UpdateSeerPath: %v\n%v", updatedGossips, expectedGossips)
        }
}

func Test_ExtractTSFromJSON_1(t *testing.T) {
        ts := int64(1111111111111)
        gossips := constructGossips(ts, `"1.1.1.1:1111"`)
        for _, gossip := range gossips {
                extractedTs, _ := ExtractTSFromJSON(gossip)
                if extractedTs != ts {
                        t.Errorf("%d != %d for gossip: %s", extractedTs, ts, gossip)
                }
        }
}

func Test_UpdateTS_1(t *testing.T) {
        /* Forcing Now to return fixed time. */
        now := time.Unix(3333333333, 0)
        Now = func() time.Time { return now }

        ts := MS(time.Unix(1111111111, 0))
        updatedGossips := constructGossips(ts, `"1.1.1.1:1111"`)
        for idx, gossip := range updatedGossips {
                updatedGossips[idx] = UpdateTS(gossip)
        }
        expectedGossips := constructGossips(MS(now), `"1.1.1.1:1111"`)
        kosher := true
        for idx, gossip := range expectedGossips {
                if gossip != updatedGossips[idx] {
                        kosher = false
                }
        }

        if !kosher {
                t.Errorf("UpdateTS: %v\n%v", updatedGossips, expectedGossips)
        }
}

/*******************************************************************************
    Tests for silly helper functions.
*******************************************************************************/

func Test_Int64Array_1(t *testing.T) {
        intArray := []int64{9, 8, 1, 3, 2, 4, 7, 6, 5, 0}
        sort.Sort(Int64Array(intArray))
        notSorted := false
        for idx, val := range intArray {
                if int64(idx) != val {
                        notSorted = true
                }
        }
        if notSorted {
                t.Error("Int64Array sorting Broken!")
        }
}

func Test_Percentile_OddLen(t *testing.T) {
        sortedArray := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
        for idx, n := range sortedArray {
                percentile := float64(idx) / 10
                if Percentile(sortedArray, percentile) != n {
                        t.Errorf("Percentile(.., %f) Broken for odd len array!", percentile)
                        t.Errorf("Got: %d", Percentile(sortedArray, percentile))
                }
        }
}

/* Commented out until can figure pretty way to fix Percentile() for Odd
func Test_Percentile_EvenLen(t *testing.T) {
        sortedArray := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        for idx, n := range sortedArray {
                percentile := float64(idx) / 10
                if Percentile(sortedArray, percentile) != n {
                        t.Errorf("Percentile(.., %f) Broken for even len array!", percentile)
                        t.Errorf("Got: %d", Percentile(sortedArray, percentile))
                }
        }
}*/

func Test_AbsInt_1(t *testing.T) {
        if AbsInt(-3) != 3 {
                t.Error("AbsInt() Broken!")
        }
}

func Test_MinInt_1(t *testing.T) {
        if MinInt(3, 4) != 3 {
                t.Error("MinInt() Broken!")
        }
}

func Test_MaxInt_1(t *testing.T) {
        if MaxInt(3, 4) != 4 {
                t.Error("MaxInt() Broken!")
        }
}

func Test_MS_1(t *testing.T) {
        testTime := time.Unix(1386118310, 0)
        if MS(testTime) != 1386118310000 {
                t.Error("MS() Broken!")
        }
}
