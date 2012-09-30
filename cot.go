package cot

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"encoding/json"
	"io/ioutil"
)

type Database struct {
	Server  string
	Name    string
	Panicky bool
}

type ViewQuery struct {
	Design     string
	Name       string
	MapDef     string
	ReduceDef  string

	StartKey   interface{}
	EndKey     interface{}
}

type ViewQueryRow struct {
	ID    string       `json:"id"`
	Key   interface{}  `json:"key"`
	Value interface{}  `json:"value"`
	Doc   interface{}  `json:"doc"`
}

type viewQueryResult struct {
	TotalRows  int         `json:"total_rows"`
	Offset     int         `json:"offset"`
	Rows       interface{} `json:"rows"`
}

func (db *Database) GetDoc(id string, dest interface{}) (bool, error) {
	resp, err := http.Get(db.Server + "/" + db.Name + "/" + id)
	if err != nil {
		if db.Panicky { panic(err) }
		return false, err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected status code %v from couchdb", resp.StatusCode)
		if db.Panicky { panic(err) }
		return false, err
	}
	
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if db.Panicky { panic(err) }
		return false, err
	}

	err = json.Unmarshal(body, dest)
	if err != nil {
		if db.Panicky { panic(err) }
		return false, err
	}

	return true, nil
}

func (db *Database) PutDoc(id string, doc interface{}) (string, error) {
	encoded, err := json.Marshal(doc)
	if err != nil {
		if db.Panicky { panic(err) }
		return "", err
	}
	
	client := &http.Client{}
	
	req, err := http.NewRequest("PUT", db.Server + "/" + db.Name + "/" + id, bytes.NewReader(encoded))
	if err != nil {
		if db.Panicky { panic(err) }
		return "", err
	}
	
	req.Header.Add("content-type", "application/json")
	
	resp, err := client.Do(req)
	if err != nil {
		if db.Panicky { panic(err) }
		return "", err
	}
	defer resp.Body.Close()
	
	switch resp.StatusCode {
	case http.StatusConflict:
		return "", nil
	case http.StatusCreated:
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			if db.Panicky { panic(err) }
			return "", err
		}
		var responseMap map[string]interface{}
		err = json.Unmarshal(body, &responseMap)
		if err != nil {
			if db.Panicky { panic(err) }
			return "", err
		}
		return responseMap["rev"].(string), nil
	}

	err = fmt.Errorf("unexpected status code %v from couchdb", resp.StatusCode)
	if db.Panicky { panic(err) }
	return "", err
}

func (db *Database) UUID() (string, error) {
	resp, err := http.Get(db.Server + "/_uuids")
	if err != nil {
		if db.Panicky { panic(err) }
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if db.Panicky { panic(err) }
		return "", err
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		if db.Panicky { panic(err) }
		return "", err
	}

	return result["uuids"].([]interface{})[0].(string), nil
}

func (db *Database) Query(query *ViewQuery, destRows interface{}) (int, error) {
	viewPath := db.Server + "/" + db.Name + "/_design/" + query.Design + "/_view/" + query.Name
	
	queryValues := url.Values{}

	if query.StartKey != nil {
		s, err := json.Marshal(query.StartKey)
		if err != nil {
			if db.Panicky { panic(err) }
			return 0, err
		}
		queryValues.Set("startkey", string(s))
	}

	if query.EndKey != nil {
		s, err := json.Marshal(query.EndKey)
		if err != nil {
			if db.Panicky { panic(err) }
			return 0, err
		}
		queryValues.Set("endkey", string(s))
	}

	resp, err := http.Get(viewPath + "?" + queryValues.Encode())
	if err != nil {
		if db.Panicky { panic(err) }
		return 0, err
	}

	if resp.StatusCode == http.StatusNotFound && query.MapDef != "" {
		resp.Body.Close()

		err = db.initView(query);
		if err != nil {
			if db.Panicky { panic(err) }
			return 0, err
		}

		resp, err = http.Get(viewPath + "?" + queryValues.Encode())
		if err != nil {
			if db.Panicky { panic(err) }
			return 0, err
		}
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected status code %v from couchdb", resp.StatusCode)
		if db.Panicky { panic(err) }
		return 0, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if db.Panicky { panic(err) }
		return 0, err
	}

	var result viewQueryResult
	result.Rows = destRows

	err = json.Unmarshal(body, &result)
	if err != nil {
		if db.Panicky { panic(err) }
		return 0, err
	}

	return result.Offset, nil
}

func (db *Database) initView(query *ViewQuery) (err error) {
	view := make(map[string]interface{})
	view["map"] = query.MapDef
	if query.ReduceDef != "" {
		view["reduce"] = query.ReduceDef
	}

	views := map[string]interface{}{
		query.Name : view,
	}

	doc := map[string]interface{}{
		"_id"   : "_design/" + query.Design,
		"views" : views,
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		return
	}
	
	client := &http.Client{}
	
	req, err := http.NewRequest("PUT", db.Server + "/" + db.Name + "/_design/" + query.Design, bytes.NewReader(encoded))
	if err != nil {
		return
	}
	
	req.Header.Add("content-type", "application/json")
	
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		err = fmt.Errorf("unexpected status code %v from couchdb", resp.StatusCode)
		return
	}

	return
}
