## 選股小工具：多家外資同買

### 篩選條件：至少三家外資有買入，且賣超前五大皆非外資

### Demo 影片
https://www.youtube.com/watch?v=kvYui3vELXg

### Fix: 憑證問題
在家用網路環境測試，沒有憑證問題
但換到特定私有網路環境，遇到了憑證問題（顯示抓取失敗）
這時可以修改程式：
```
response = requests.get(self.url, headers=self.headers, timeout=30)
```
改為
```
response = requests.get(self.url, headers=self.headers, timeout=30, verify=False)
```
便會讓 requests 套件忽略 SSL 憑證檢查，即可抓取：外資買賣超資料
PS: 資料來源是富邦券商分點買賣超報表，只是富邦回傳的憑證鏈不完全符合規範（缺 SKI）所以有時會有此問題
