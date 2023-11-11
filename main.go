package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-resty/resty/v2"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	urlSession = "https://development.kpi-drive.ru/_api/auth/login"
	urlArango  = "https://development.kpi-drive.ru/_api/events"
	urlMysql   = "https://development.kpi-drive.ru/_api/facts/save_fact"

	bearerToken = "48ab34464a5573519725deb5865cc74c"
)

func main() {
	client := NewRestyGetClient()

	loginRequest := map[string]string{
		"login":    "admin",
		"password": "admin",
	}

	response, err := client.Request(urlSession, loginRequest, resty.MethodGet, "")
	if err != nil {
		log.Print("Ошибка при запросе сессии : " + err.Error())
		return
	}

	// Формируем запрос в ArangoDB, для простоты используем текстовое значение,
	// но желательно структуру
	request := `{ 	"filter": {"field": {"key": "type",  "sign": "LIKE",  "values": ["MATRIX_REQUEST"]}},
					"sort": {"fields": [ "time" ], "direction": "DESC" },
					"limit": 10 }`

	dataRows, err := GetArangoRequest(urlArango, response.Cookies()[0].Raw, request)
	if err != nil {
		log.Print("Ошибка при запросе GetArangoRequest : " + err.Error())
		return
	}

	// В примере запроса в ArangoDB указан лимит 10, но судя по запросу в MySQL, нужно записать одну строку
	// на всякий случай проверим, что интересующие поля (с точки зрения передачи в MySQL) во всех 10 записях идентичны
	setUserName := map[string]struct{}{}
	setDate := map[string]struct{}{}
	setParams := map[string]struct{}{}

	for _, row := range dataRows {
		setUserName[row.Author.UserName] = struct{}{}
		setDate[row.Time.Format("2006-01-02")] = struct{}{}
		setParams[PrettyJSON(row.Params)] = struct{}{}
	}

	// Если кол-во ключевых параметров отличается, вызовем панику
	if len(setUserName) != 1 {
		log.Panicf("Найдено 0 или несколько user_name :%d", len(setUserName))
	}
	if len(setDate) != 1 {
		log.Panicf("Найдено 0 или несколько time :%d", len(setDate))
	}
	if len(setParams) != 1 {
		log.Panicf("Найдено 0или несколько params :%d", len(setParams))
	}

	// Если все как ожидали формируем запрос в БД, согласно условиям
	mysqlRquest := map[string]string{
		"period_start":            dataRows[0].Params.Period.Start, // как понимаю нас интересует именно период указанный в парамсах
		"period_end":              dataRows[0].Params.Period.End,
		"period_key":              "month",
		"indicator_to_mo_id":      "315914",
		"indicator_to_mo_fact_id": "0",
		"value":                   "1",
		"fact_time":               dataRows[0].Time.Format("2006-01-02"),
		"is_plan":                 "0",
		"supertags":               fmt.Sprintf(`:[{"tag":{"id":2,"name":"Клиент","key":"client","values_source":0},"value":"%s"}]`, dataRows[0].Author.UserName),
		"auth_user_id":            "40",
		"comment":                 PrettyJSON(dataRows[0].Params)}

	// Записываем данные в MySQL
	response, err = client.Request(urlMysql, mysqlRquest, resty.MethodPost, bearerToken)
	if err != nil {
		log.Print("Ошибка при записи в в MySQL: " + err.Error())
		return
	}

}

// GetArangoRequest запрос в Аранго БД
// ввиду достаточно "экзотического" GET запроса с телом, и того что библиотека go-resty
// не позволяет удобно отправлять GET запроса с телом, то в качестве исключения использую net/http
// в реальной ситуации, использовал бы везде go-resty, и настаивал бы на установленных стандартах
func GetArangoRequest(url, cookie, data string) ([]DataRow, error) {
	payload := strings.NewReader(data)
	req, err := http.NewRequest("GET", url, payload)
	if err != nil {
		log.Printf("Ошибка при создании запроса GetArangoRequest : %s", err.Error())
		return nil, err
	}

	req.Header.Add("cookie", cookie)
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Ошибка при запросе GetArangoRequest Do : %s", err.Error())
		return nil, err
	}

	defer res.Body.Close()
	log.Printf("Сервер вернул %s на запрос на %s", res.Status, url)

	// Если запрос не успешный вернем ошибку, чтобы не продолжать дальше
	if res.StatusCode != 200 {
		return nil, errors.New("неуспешный запрос Аранго БД, статус не ОК")
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("Ошибка io.ReadAll GetArangoRequest  %s", err)
		return nil, err
	}

	// Сконфигурируем ответ на структуру
	var arangoResponse ArangoResponse

	err = json.Unmarshal(body, &arangoResponse)
	if err != nil {
		log.Printf("Ошибка при Unmarshal  ArangoResponse : %s", err.Error())
		return nil, err
	}

	return arangoResponse.DATA.Rows, nil
}

// // // // // //
//  CLIENTS   //
// // // //  //

// RestyGetClient клиент для гет запросов
type RestyGetClient struct {
	restyGetClient *resty.Client
}

// NewRestyGetClient конструктор для RestyGetClient
func NewRestyGetClient() *RestyGetClient {
	return &RestyGetClient{
		restyGetClient: resty.New()}
}

// Request получаем куки с данными сессии
func (с *RestyGetClient) Request(url string, payload map[string]string, method, token string) (*resty.Response, error) {
	var payloadGet map[string]string
	var payloadPost map[string]string

	// Подготовим пейлоад для нужного типа
	switch method {
	case resty.MethodGet:
		payloadGet = payload
	case resty.MethodPost:
		payloadPost = payload
	default:
		return nil, errors.New("неизвестный (неожиданный) тип запроса")

	}

	resp, err := с.restyGetClient.R().
		SetQueryParams(payloadGet).
		SetFormData(payloadPost).
		SetAuthToken(token).
		SetHeader("Content-Type", "multipart/form-data;").
		SetDebug(false).
		SetContentLength(true).
		Execute(method, url)
	if err != nil {
		log.Print("Ошибка при Request запросе : " + err.Error())
	}

	log.Printf("Сервер вернул %s на запрос на %s", resp.Status(), url)
	// Если запрос не успешный вернем ошибку, чтобы не продолжать дальше
	if resp.StatusCode() != 200 {
		return nil, errors.New("неуспешный Request, статус не OK")
	}

	return resp, nil
}

// // // // //
//  UTILS  //
// // //  //

// PrettyJSON функция для получения строки JSON
func PrettyJSON(i interface{}) string {
	res, err := json.MarshalIndent(i, " ", "\t")
	if err != nil {
		log.Panicf("PrettyJSON marshalling is failed: %v", err.Error())
	}
	return string(res)
}

// // // // //
//   DTOs  //
// // //  //

// ArangoResponse структра ответа
type ArangoResponse struct {
	MESSAGES struct {
		Error   interface{} `json:"error"`
		Warning interface{} `json:"warning"`
		Info    interface{} `json:"info"`
	} `json:"MESSAGES"`
	DATA struct {
		Page       int       `json:"page"`
		PagesCount int       `json:"pages_count"`
		RowsCount  int       `json:"rows_count"`
		Rows       []DataRow `json:"rows"`
	} `json:"DATA"`
	STATUS string `json:"STATUS"`
}

// DataRow структура записи
type DataRow struct {
	Id     string `json:"_id"`
	Key    string `json:"_key"`
	Rev    string `json:"_rev"`
	Author struct {
		MoId     int    `json:"mo_id"`
		UserId   int    `json:"user_id"`
		UserName string `json:"user_name"`
	} `json:"author"`
	Group  string `json:"group"`
	Msg    string `json:"msg"`
	Params struct {
		IndicatorToMoId int `json:"indicator_to_mo_id"`
		Period          struct {
			End     string `json:"end"`
			Start   string `json:"start"`
			TypeId  int    `json:"type_id"`
			TypeKey string `json:"type_key"`
		} `json:"period"`
		Platform string `json:"platform"`
	} `json:"params"`
	Time time.Time `json:"time"`
	Type string    `json:"type"`
}
