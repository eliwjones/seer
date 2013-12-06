package main

import (
        "fmt"
        "sort"
        "testing"
        "time"
)

func constructGossips(ts int64, seerPath string, tombstone string) []string {
        propmap := map[string]string{
                `"SeerAddr"`:    `"1.1.1.1:1111"`,
                `"SeerRequest"`: `"Metadata"`,
        }
        propmap[`"TS"`] = fmt.Sprintf(`%d`, ts)
        if seerPath != `` {
                propmap[`"SeerPath"`] = fmt.Sprintf(`[%s]`, seerPath)
        }
        if tombstone != `` {
                propmap[`"Tombstone"`] = "true"
        }
        keys := []string{}
        for k, _ := range propmap {
                keys = append(keys, k)
        }
        keycombinations := GetCombinations(keys)
        gossips := []string{}
        for _, keycombo := range keycombinations {
                gossip := ``
                for _, propname := range keycombo {
                        gossip = fmt.Sprintf(`%s,%s:%s`, gossip, propname, propmap[propname])
                }
                gossip = fmt.Sprintf(`{%s}`, gossip[1:])
                gossips = append(gossips, gossip)
        }

        return gossips
}

func getUniqueArray(array []string) []string {
        uniquifier := map[string]bool{}
        for _, item := range array {
                uniquifier[item] = true
        }
        result := []string{}
        for item, _ := range uniquifier {
                result = append(result, item)
        }
        return result
}

func setsEqual(arrayOne []string, arrayTwo []string) bool {
        arrayOneTracker := map[string]bool{}
        arrayTwoTracker := map[string]bool{}
        for _, item := range arrayOne {
                arrayOneTracker[item] = true
        }
        for _, item := range arrayTwo {
                arrayTwoTracker[item] = true
                if !arrayOneTracker[item] {
                        return false
                }
        }
        for item, _ := range arrayOneTracker {
                if !arrayTwoTracker[item] {
                        return false
                }
        }
        return true
}

/*******************************************************************************
    Tests for Regex functions.
*******************************************************************************/

func Test_UpdateSeerPath(t *testing.T) {
        seerPath := `"1.1.1.1:1111","2.2.2.2:2222"`
        newSeer := `"3.3.3.3:3333"`
        ts := int64(1111111111111)
        updatedGossips := constructGossips(ts, seerPath, ``)

        for idx, gossip := range updatedGossips {
                updatedGossips[idx] = UpdateSeerPath(gossip, newSeer)
        }

        seerPath = newSeer + `,` + seerPath
        expectedGossips := constructGossips(ts, seerPath, ``)
        if !setsEqual(expectedGossips, updatedGossips) {
                t.Errorf("UpdateSeerPath: %v\n%v", getUniqueArray(updatedGossips), getUniqueArray(expectedGossips))
        }
}

func Test_RemoveSeerPath(t *testing.T) {
        ts := int64(1111111111111)
        updatedGossips := constructGossips(ts, `"1.1.1.1:1111","2.2.2.2:2222"`, ``)
        for idx, gossip := range updatedGossips {
                updatedGossips[idx] = RemoveSeerPath(gossip)
        }
        expectedGossips := constructGossips(ts, ``, ``)
        if !setsEqual(expectedGossips, updatedGossips) {
                t.Errorf("RemoveSeerPath gave:%v\n%v", getUniqueArray(updatedGossips), getUniqueArray(expectedGossips))
        }
}

func Test_RemoveTombstone(t *testing.T) {
        ts := int64(1111111111111)
        seerPath := `"1.1.1.1:1111","2.2.2.2:2222"`
        updatedGossips := constructGossips(ts, seerPath, `true`)
        for idx, gossip := range updatedGossips {
                updatedGossips[idx] = RemoveTombstone(gossip)
        }
        expectedGossips := constructGossips(ts, seerPath, ``)
        if !setsEqual(expectedGossips, updatedGossips) {
                t.Errorf("RemoveTombstone gave:%v\n%v", getUniqueArray(updatedGossips), getUniqueArray(expectedGossips))
        }
}

func Test_ExtractTSFromJSON_1(t *testing.T) {
        ts := int64(1111111111111)
        gossips := constructGossips(ts, `"1.1.1.1:1111"`, ``)
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
        updatedGossips := constructGossips(ts, `"1.1.1.1:1111"`, ``)
        for idx, gossip := range updatedGossips {
                updatedGossips[idx] = UpdateTS(gossip)
        }
        expectedGossips := constructGossips(MS(now), `"1.1.1.1:1111"`, ``)
        if !setsEqual(expectedGossips, updatedGossips) {
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

func Test_GetFilenameAndDir(t *testing.T) {
        fullpath := `/iam/a/full/path/and/filename.f`
        filename, dir := GetFilenameAndDir(fullpath)
        if filename != `filename.f` || dir != `/iam/a/full/path/and` {
                t.Errorf("[GetFilenameAndDir] fullpath: %s, filename: %s, dir: %s", fullpath, filename, dir)
        }
}

func Test_GetCombinations_1(t *testing.T) {
        keys := []string{"1", "2", "3"}
        result := GetCombinations(keys)
        expectedResult := [][]string{
                []string{"3", "2", "1"},
                []string{"2", "3", "1"},
                []string{"2", "1", "3"},
                []string{"3", "1", "2"},
                []string{"1", "3", "2"},
                []string{"1", "2", "3"},
        }
        kosher := true
        for i, _ := range result {
                for j, _ := range result[i] {
                        if result[i][j] != expectedResult[i][j] {
                                kosher = false
                        }
                }
        }
        if !kosher {
                t.Errorf("%v", result)
        }
}

func Test_Percentile_OddLen(t *testing.T) {
        sortedArray := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}
        length := float64(len(sortedArray))
        for idx, n := range sortedArray {
                percentile := float64(idx) / length
                if Percentile(sortedArray, percentile) != n {
                        t.Errorf("Percentile(.., %f) Broken for odd len array!", percentile)
                        t.Errorf("Got: %d, for idx: %d", Percentile(sortedArray, percentile), idx)
                }
        }
}

func Test_Percentile_EvenLen(t *testing.T) {
        sortedArray := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}
        length := float64(len(sortedArray))
        for idx, n := range sortedArray {
                percentile := float64(idx) / length
                if Percentile(sortedArray, percentile) != n {
                        t.Errorf("Percentile(.., %f) Broken for even len array!", percentile)
                        t.Errorf("Got: %d, for idx: %d", Percentile(sortedArray, percentile), idx)
                }
        }
}

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
