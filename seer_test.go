package main

import (
        "archive/tar"
        "bytes"
        "compress/gzip"
        "fmt"
        "io"
        "io/ioutil"
        "os"
        "sort"
        "strings"
        "testing"
        "time"
)

/*******************************************************************************
    Test Helpers.
*******************************************************************************/

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
    Tests for file IO related functions.
*******************************************************************************/

func Test_createTarGz(t *testing.T) {
        // World's most painful unittest.  Feels like I am Doing It Wrong.
        tarpath := "/tmp/go-createTarGz-test.tar.gz"

        err := createTarGz(tarpath, "/badpath/to/a/folder/to/gz", "/badpath/to/another/folder/to/gz")
        if err == nil {
                t.Error("Should have received an error but I did not!!")
        }
        folders := map[string]string{
                "file1": SeerDirs["data"] + "/testdir1/subdir",
                "file2": SeerDirs["data"] + "/testdir2/subdir",
        }
        for filename, folder := range folders {
                os.MkdirAll(folder, 0777)
                fullpath := folder + "/" + filename
                ioutil.WriteFile(fullpath, []byte(fmt.Sprintf(`%s`, fullpath)), 0777)
                defer os.RemoveAll(strings.Replace(folder, "/subdir", "", -1))
        }

        err = createTarGz(tarpath, strings.Replace(folders["file1"], "/subdir", "", -1), strings.Replace(folders["file2"], "/subdir", "", -1))

        if err != nil {
                t.Errorf("Received a createTarGz() err: %v", err)
                return
        }
        defer os.Remove(tarpath)
        // Open tar and verify files. Ugggg.
        targzfile, err := os.Open(tarpath)
        if err != nil {
                t.Errorf("Received an os.Open() err: %v", err)
                return
        }
        defer targzfile.Close()
        gzr, err := gzip.NewReader(targzfile)
        if err != nil {
                t.Errorf("Received a gzip.NewReader() err: %v", err)
                return
        }
        defer gzr.Close()
        tr := tar.NewReader(gzr)
        extractedFiles := map[string]string{}
        for {
                hdr, err := tr.Next()
                if err == io.EOF {
                        break
                }
                buff := bytes.NewBuffer(nil)
                filename, dir := GetFilenameAndDir(hdr.Name)
                io.Copy(buff, tr)
                extractedFiles[filename] = strings.Replace(buff.String(), "/"+filename, "", -1)
                // Verify dir from hdr.Name is "appropriate".
                if !strings.HasSuffix(folders[filename], dir) || strings.HasPrefix(dir, SeerDirs["data"]) {
                        t.Errorf("Extracted dir is wrong! Got: %s, Expected: %s", dir, folders[filename])
                        return
                }
        }
        for filename, folder := range folders {
                if folder != extractedFiles[filename] {
                        t.Errorf("Filename: %s Mismatch. Got: %s, Expected: %s", filename, extractedFiles[filename], folder)
                }
        }
}

func Test_processSeed(t *testing.T) {
        // Contender for World's Ugliest Unittest.
        tarpath := "/tmp/go-processSeed-test.tar.gz"
        tarfile, err := os.Create(tarpath)
        if err != nil {
                t.Errorf("Error creating tarfile")
        }
        // Hacky since too lazy to learn how to manually set file permissions in tar header.
        tarfileinfo, err := tarfile.Stat()
        if err != nil {
                t.Errorf("Error stat-ing tarfile.")
        }
        defer os.Remove(tarpath)
        gw, err := gzip.NewWriterLevel(tarfile, gzip.BestCompression)
        if err != nil {
                t.Errorf("Error creating gzip Writer.")
        }
        defer gw.Close()
        tw := tar.NewWriter(gw)
        defer tw.Close()
        var files = []struct {
                Name, Body string
        }{
                {"testdir1/subdir/file1", "seer/127.0.0.9:9999/gossip/data/testdir1/subdir/file1"},
                {"testdir2/subdir/file2", "seer/127.0.0.9:9999/gossip/data/testdir2/subdir/file2"},
        }
        for _, file := range files {
                hdr := &tar.Header{
                        Name:   file.Name,
                        Size:   int64(len(file.Body)),
                        Mode:   int64(tarfileinfo.Mode()),
                }
                err = tw.WriteHeader(hdr)
                if err != nil {
                }
                _, err = tw.Write([]byte(file.Body))
                if err != nil {
                }
        }
        tw.Close()
        gw.Close()

        processSeed(tarpath)
        defer os.RemoveAll(SeerDirs["data"] + "/testdir1")
        defer os.RemoveAll(SeerDirs["data"] + "/testdir2")
        // Verify files are found.
        for _, file := range files {
                // file.Name == path
                // file.Body == stuff
                processedPath := SeerDirs["data"] + "/" + file.Name
                processedFile, err := ioutil.ReadFile(processedPath)
                if err != nil {
                        t.Errorf("Error finding processedPath: %s", processedPath)
                }
                if string(processedFile) != file.Body {
                        t.Errorf("File Contents mismatch: [%s] != [%s]", string(processedFile), file.Body)
                }
        }
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
        // Forcing Now to return fixed time.
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
