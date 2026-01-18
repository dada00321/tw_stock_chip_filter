"""
券商籌碼選股程式
"""

import requests
from lxml import html
import pandas as pd
import re
import time
import os


class BrokerCrawler:
    """券商資料爬蟲"""
    
    def __init__(self, url):
        self.url = url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.7',
        }
        self.tree = None
        self.html_content = None
    
    def fetch_page(self):
        """抓取網頁"""
        try:
            response = requests.get(self.url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # 嘗試不同編碼
            for encoding in ['big5', 'cp950', 'gb2312', 'utf-8']:
                try:
                    self.html_content = response.content.decode(encoding)
                    return True
                except UnicodeDecodeError:
                    continue
            
            # 最後嘗試忽略錯誤
            self.html_content = response.content.decode('big5', errors='ignore')
            return True
            
        except Exception as e:
            print(f"❌ 抓取失敗: {e}")
            return False
    
    def parse_html(self):
        """解析 HTML"""
        if not self.html_content:
            return False
        try:
            self.tree = html.fromstring(self.html_content)
            return True
        except:
            return False
    
    def extract_stock_id(self, script_text):
        """提取股票代號"""
        match = re.search(r"GenLink2stk\('AS(\d+)'", script_text)
        return match.group(1) if match else None
    
    def extract_stock_name(self, script_text):
        """提取股票名稱"""
        match = re.search(r"GenLink2stk\('AS\d+','([^']+)'", script_text)
        return match.group(1) if match else None
    
    def parse_number(self, text):
        """轉換數字"""
        if not text:
            return 0
        clean_text = text.replace(',', '').replace(' ', '').replace('%', '').strip()
        try:
            return int(clean_text)
        except ValueError:
            return 0
    
    def crawl_chip_data(self, action):
        """抓取買超或賣超資料"""
        if self.tree is None:
        #if not self.tree:
            return None
        
        headers = self.tree.xpath(f"//td[@class='t2' and text()='{action}']")
        if not headers:
            return None
        
        all_data = []
        for header in headers:
            parent_table = header.xpath("ancestor::table[@class='t0'][1]")[0]
            data_rows = parent_table.xpath(".//tr[position() > 2]")
            
            for row in data_rows:
                try:
                    script_elements = row.xpath(".//script")
                    if not script_elements:
                        continue
                    
                    script_text = script_elements[0].text_content()
                    stock_id = self.extract_stock_id(script_text)
                    stock_name = self.extract_stock_name(script_text)
                    number_cells = row.xpath(".//td[@class='t3n1']")
                    
                    if len(number_cells) >= 3:
                        diff_amount = self.parse_number(number_cells[2].text_content())
                        all_data.append({
                            'stock_id': stock_id,
                            'stock_name': stock_name,
                            'diff_amount': diff_amount
                        })
                except:
                    continue
        
        return all_data
    
    def crawl_stock_detail(self):
        """
        抓取個股券商進出明細（正確版）
        結構：買超和賣超在同一個 TR 中
        
        HTML 結構：
        <TR>
            <TD>買超券商名稱</TD>      <- cells[0]
            <TD>買進</TD>             <- cells[1]
            <TD>賣出</TD>             <- cells[2]
            <TD>買超</TD>             <- cells[3]
            <TD>佔成交比重</TD>         <- cells[4]
            <TD>賣超券商名稱</TD>      <- cells[5]
            <TD>買進</TD>             <- cells[6]
            <TD>賣出</TD>             <- cells[7]
            <TD>賣超</TD>             <- cells[8]
            <TD>佔成交比重</TD>         <- cells[9]
        </TR>
        
        返回: {'buy_top5': [...], 'sell_top5': [...]}
        """
        if self.tree is None:
        #if not self.tree:
            return None
        
        result = {'buy_top5': [], 'sell_top5': []}
        
        # 找到包含「買超券商」和「賣超券商」標題的 TR
        header_row = self.tree.xpath("//td[@class='t2' and text()='買超券商']")
        
        if not header_row:
            return result
        
        # 找到標題所在的 TR，然後找後續的兄弟 TR（資料列）
        parent_tr = header_row[0].xpath("ancestor::tr[1]")[0]
        
        # 找到所有後續的資料列
        data_rows = parent_tr.xpath("following-sibling::tr")
        
        buy_list = []
        sell_list = []
        
        for row in data_rows:
            cells = row.xpath(".//td")
            
            # 至少需要 10 個欄位（買超5個 + 賣超5個）
            if len(cells) < 10:
                continue
            
            try:
                # 解析買超資料（前5個欄位）
                buy_broker_elem = cells[0].xpath(".//a")
                if buy_broker_elem:
                    buy_broker_name = buy_broker_elem[0].text_content().strip()
                    buy_amount = self.parse_number(cells[1].text_content())
                    sell_amount = self.parse_number(cells[2].text_content())
                    diff_amount = self.parse_number(cells[3].text_content())
                    
                    # 只記錄有效的買超資料
                    if buy_broker_name and diff_amount != 0:
                        buy_list.append({
                            'broker_name': buy_broker_name,
                            'buy': buy_amount,
                            'sell': sell_amount,
                            'diff': diff_amount
                        })
            except:
                pass
            
            try:
                # 解析賣超資料（後5個欄位）
                sell_broker_elem = cells[5].xpath(".//a")
                if sell_broker_elem:
                    sell_broker_name = sell_broker_elem[0].text_content().strip()
                    buy_amount = self.parse_number(cells[6].text_content())
                    sell_amount = self.parse_number(cells[7].text_content())
                    diff_amount = self.parse_number(cells[8].text_content())
                    
                    # 賣超的差額應該是負數，但網頁上可能只顯示絕對值
                    # 確保賣超是負數
                    if diff_amount > 0:
                        diff_amount = -diff_amount
                    
                    # 只記錄有效的賣超資料
                    if sell_broker_name and diff_amount != 0:
                        sell_list.append({
                            'broker_name': sell_broker_name,
                            'buy': buy_amount,
                            'sell': sell_amount,
                            'diff': diff_amount
                        })
            except:
                pass
        
        # 取前5名
        result['buy_top5'] = buy_list[:5]
        result['sell_top5'] = sell_list[:5]
        
        return result


def download_broker_chips(trans_date, output_dir='data'):
    """步驟 1: 下載各券商買賣超資料"""
    
    print(f"\n{'='*70}")
    print(f"📥 步驟 1: 下載券商買賣超資料")
    print(f"{'='*70}")
    
    # 日期格式轉換
    year, month, day = trans_date.split('-')
    formatted_date = f"{year}-{int(month)}-{int(day)}"
    
    # 外資券商清單
    brokers = {
        "台灣摩根士丹利": 1470, 
        "摩根大通": 8440,
        "美商高盛": 1480, 
        "美林": 1440,
        "花旗環球": 1590,
        "法銀巴黎": 8900,
        "新加坡商瑞銀": 1650,
        "香港上海匯豐": 8960
    }
    
    all_broker_data = {}
    
    # 逐個券商抓取
    for broker_name, broker_id in brokers.items():
        print(f"  📊 {broker_name}...", end=' ')
        
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={broker_id}&b={broker_id}&c=B&e={formatted_date}&f={formatted_date}"
        crawler = BrokerCrawler(url)
        
        if not crawler.fetch_page() or not crawler.parse_html():
            print("❌")
            time.sleep(1)
            continue
        
        buy_data = crawler.crawl_chip_data("買超")
        sell_data = crawler.crawl_chip_data("賣超")
        
        if buy_data or sell_data:
            combined_data = []
            if buy_data:
                for item in buy_data:
                    item['type'] = '買超'
                    combined_data.append(item)
            if sell_data:
                for item in sell_data:
                    item['type'] = '賣超'
                    combined_data.append(item)
            
            all_broker_data[broker_name] = pd.DataFrame(combined_data)
            print(f"✅ ({len(combined_data)} 筆)")
        else:
            print("⚠️")
        
        time.sleep(2)
    
    # 儲存 Excel
    if all_broker_data:
        output_file = f"{output_dir}/chip_{trans_date}.xlsx"
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            all_data = []
            for broker_name, df in all_broker_data.items():
                df_copy = df.copy()
                df_copy['broker'] = broker_name
                all_data.append(df_copy)
            
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df[['broker', 'type', 'stock_id', 'stock_name', 'diff_amount']]
            combined_df.columns = ['券商', '類型', '股票代號', '股票名稱', '買賣超張數']
            combined_df.to_excel(writer, sheet_name='全部資料', index=False)
        
        print(f"\n✅ 已儲存: {output_file}")
        print(f"📊 總資料: {len(combined_df)} 筆")
        return output_file
    
    return None


def filter_strong_buy_stocks(chip_file):
    """步驟 2: 篩選符合條件的股票"""
    
    print(f"\n{'='*70}")
    print(f"🔍 步驟 2: 篩選 「至少 3 家外資買超」 的股票")
    print(f"{'='*70}")
    
    df = pd.read_excel(chip_file, sheet_name='全部資料')
    
    # 建立賣超池
    sell_pool = set(df[df["類型"] == "賣超"]["股票名稱"])
    print(f"  🚫 賣超股票數: {len(sell_pool)}")
    
    # 篩選：買超次數 > 3 且不在賣超池
    buy_df = df[df["類型"] == "買超"]
    buy_result = buy_df.groupby("股票名稱")\
                 .filter(lambda x: len(x) >= 3 and x.name not in sell_pool)["股票代號"]\
                 .drop_duplicates()\
                 .tolist()
    
    print(f"  ✅ 符合條件的股票: {len(buy_result)} 檔")
    if buy_result:
        print(f"     {', '.join(str(s) for s in buy_result[:10])}{'...' if len(buy_result) > 10 else ''}")
    
    
    tmp = [df[df["股票代號"]==stock_id]["股票名稱"].iloc[0] for stock_id in buy_result]
    buy_result_v2 = {"股票代號": buy_result,
                     "股票名稱": tmp}
    return buy_result_v2


def crawl_stock_details(stock_dict, trans_date):
    """步驟 3: 抓取個股券商進出明細"""
    
    print(f"\n{'='*70}")
    print(f"📥 步驟 3: 抓取個股券商進出明細")
    print(f"{'='*70}")
    
    year, month, day = trans_date.split('-')
    formatted_date = f"{year}-{int(month)}-{int(day)}"
    
    # 外資券商名稱（用於判斷）
    foreign_brokers = {
        "台灣摩根士丹利", "摩根大通", "美商高盛", "美林", 
        "花旗環球", "法銀巴黎", "新加坡商瑞銀", "香港上海匯豐"
    }
    
    stock_details = []
    
    stock_list = stock_dict["股票代號"]
    stock_names = stock_dict["股票名稱"]
    for idx, stock_id in enumerate(stock_list, 1):
        # 取得股票名稱
        stock_name = stock_names[idx-1]
        # stock_name_elem = crawler.tree.xpath("//span[@class='t3n1']")
        # stock_name = stock_name_elem[0].text_content().strip() if stock_name_elem else stock_id
        print(f"  [{idx}/{len(stock_list)}] {stock_id} {stock_name} ...", end=' ')
        
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={stock_id}&e={formatted_date}&f={formatted_date}"
        crawler = BrokerCrawler(url)
        
        if not crawler.fetch_page() or not crawler.parse_html():
            print("❌")
            time.sleep(1)
            continue
        
        detail = crawler.crawl_stock_detail()
        
        if not detail:
            print("⚠️ (無資料)")
            time.sleep(1)
            continue
        
        # 檢查是否有賣超資料
        if not detail['sell_top5']:
            print("✅ (無賣超)")
            # 沒有賣超也算符合條件
        else:
            # 檢查賣超前五大是否有外資
            sell_top5_brokers = [b['broker_name'] for b in detail['sell_top5']]
            has_foreign = any(
                any(foreign in broker for foreign in foreign_brokers)
                for broker in sell_top5_brokers
            )
            
            if has_foreign:
                print("🚫 (有外資賣超)")
                time.sleep(1)
                continue
        
        
        # 整理買超和賣超前五大資料
        buy_top5_names = [b['broker_name'] for b in detail.get('buy_top5', [])]
        buy_top5_amounts = [str(b['diff']) for b in detail.get('buy_top5', [])]
        sell_top5_names = [b['broker_name'] for b in detail.get('sell_top5', [])]
        sell_top5_amounts = [str(abs(b['diff'])) for b in detail.get('sell_top5', [])]  # 取絕對值
        
        stock_details.append({
            'stock_id': stock_id,
            'stock_name': stock_name,
            'buy_top5_names': ','.join(buy_top5_names) if buy_top5_names else '-',
            'buy_top5_amounts': ','.join(buy_top5_amounts) if buy_top5_amounts else '-',
            'sell_top5_names': ','.join(sell_top5_names) if sell_top5_names else '-',
            'sell_top5_amounts': ','.join(sell_top5_amounts) if sell_top5_amounts else '-'
        })
        
        print("✅")
        time.sleep(2)
    
    print(f"\n✅ 最終篩選出: {len(stock_details)} 檔股票")
    
    return stock_details


def save_filtered_results(stock_details, trans_date, output_dir='data'):
    """步驟 4: 儲存最終結果"""
    
    print(f"\n{'='*70}")
    print(f"💾 步驟 4: 儲存篩選結果")
    print(f"{'='*70}")
    
    if not stock_details:
        print("  ⚠️ 沒有符合條件的股票")
        return None
    
    df = pd.DataFrame(stock_details)
    df.columns = ['股票代號', '股票名稱', '買超前五券商', '買超張數', '賣超前五券商', '賣超張數']
    
    output_file = f"{output_dir}/chip_filtered_{trans_date}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='篩選結果', index=False)
    
    print(f"  ✅ 已儲存: {output_file}")
    print(f"  📊 共 {len(df)} 檔股票")
    print(f"\n{'='*70}")
    print("前 5 檔預覽:")
    print(f"{'='*70}")
    print(df.head().to_string(index=False))
    
    return output_file


def main():
    """主程式"""
    
    # ============ 設定區 ============
    trans_date = "2026-01-16"  # 交易日期
    output_dir = "data"        # 輸出目錄
    # ===============================
    
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n" + "="*70)
    print("🚀 券商籌碼選股程式：開始執行")
    print("="*70)
    print(f"📅 交易日期: {trans_date}")
    print(f"📁 輸出目錄: {output_dir}")
    
    try:
        # 步驟 1: 下載券商買賣超資料
        chip_file = download_broker_chips(trans_date, output_dir)
        if not chip_file:
            print("\n❌ 下載失敗")
            return
        
        # 步驟 2: 篩選強力買超股票
        stock_dict = filter_strong_buy_stocks(chip_file)
        if not stock_dict:
            print("\n⚠️ 沒有符合條件的股票")
            return
        
        # 步驟 3: 抓取個股明細並過濾
        stock_details = crawl_stock_details(stock_dict, trans_date)
        
        # 步驟 4: 儲存結果
        output_file = save_filtered_results(stock_details, trans_date, output_dir)
        
        if output_file:
            print(f"\n{'='*70}")
            print("✅ 全部完成！")
            print(f"{'='*70}")
            print(f"📁 最終結果: {output_file}")
        
    except Exception as e:
        print(f"\n❌ 發生錯誤: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()