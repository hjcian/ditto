# ditto
A load testing tool for mimicking your real production traffic


基本功能
- input 
    - (done) static input: 用 JSON 定義 request 的 URL, METHOD, HEAD 與 BODY，只允許 content-type: application/json 的 API 連接
    - dynamic input: 定義一個 class 要求繼承並實作特定 method 
    - scenario input: 有點難呢，考慮有 login 與 request 的組合，如何彈性？
- output
    - (done) total stats: status count、exception count、總完成時間
    - 單位時間內的統計: status count 以及 exception count