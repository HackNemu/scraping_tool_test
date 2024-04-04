import threading
from tkinter import Tk, Button, messagebox, Label, Frame, StringVar, font
from tkinter.ttk import  Progressbar
from retry import retry
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import re
import folium
from folium.plugins import MarkerCluster
import time
import random
import webbrowser


### 定義 ###

# SUUMO_URL
BASE_URL = "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=090&bs=040&ta=40&sc=40131&sc=40132&sc=40133&sc=40134&sc=40135&sc=40136&sc=40137&cb=0.0&ct=6.5&mb=0&mt=9999999&et=10&cn=15&tc=0400502&tc=0400301&shkr1=03&shkr2=03&shkr3=03&shkr4=03&sngz=&po1=25&pc=50"
PAGE_FMT = "&page={}"
SUUMO_URL = BASE_URL + PAGE_FMT
MAX_PAGE = 2 # 取得対象のページ数

# API
KOKUDO_API = "https://msearch.gsi.go.jp/address-search/AddressSearch?q="            # 国土地理院API
ZIPCODA_API = "http://zipcoda.net/api?address="                                     # 郵便番号取得API
HRG_API =   "http://geoapi.heartrails.com/api/json?method=searchByPostal&postal="   # HeartRails Geo API

# OUTPUT
CSV_FILE_PATH = "fukuokashi_data.csv"
HTML_FILE_PATH = "fukuoka.html"

class ScraperGUI:

    ### GUI ###
    def __init__(self, base_url, max_page):
        self.base_url = base_url
        self.max_page = max_page
        self.total_records = 0  # 総件数
        self.completed_records = 0  # 処理完了の件数
        self.scraping_thread = None  # スレッド属性の追加

        self.root = Tk()
        self.root.title("不動産情報スクレイピング")
        self.root.geometry("300x150")  # ウィンドウのサイズを設定

        # フォントの設定
        self.custom_font = font.Font(family="Helvetica", size=10)

        # 開始ボタンを追加
        self.start_button = Button(self.root, text="スクレイピング開始", command=self.start_scraping, font=self.custom_font)
        self.start_button.pack(pady=5)

        # 中断ボタンを追加
        self.stop_button = Button(self.root, text="スクレイピング中断", command=self.stop_scraping_thread, font=self.custom_font)
        self.stop_button.pack(pady=5)

       # ラベル用のフレームを追加
        self.label_frame = Frame(self.root)
        self.label_frame.pack()

        # 現在の件数と総件数を表示するラベルのテキストを更新するためのStringVarを設定
        self.current_count_text = StringVar()
        self.total_count_text = StringVar()

        # 現在の件数を表示するラベルを追加
        self.current_count_label = Label(self.label_frame, textvariable=self.current_count_text)
        self.current_count_label.pack(side="left",padx=10)
        self.current_count_text.set(f"処理完了: {0}")

        # 総件数を表示するラベルを追加
        self.total_count_label = Label(self.label_frame, textvariable=self.total_count_text)
        self.total_count_label.pack(side="left",padx=10)
        self.total_count_text.set(f"総件数: {0}")

        # プログレスバーを初期化
        self.progress = Progressbar(self.root, orient="horizontal", length=200, mode="determinate",maximum=1000)
        self.progress.pack(pady=10)
        
        self.root.mainloop()

    def gui_init(self):
        self.current_count_text.set(f"処理完了: {0}")
        self.total_count_text.set(f"総件数: {0}")
        self.update_progressbar(0)


    def start_scraping(self):
        if not self.scraping_thread:
            self.stop_scraping_flg = False  # スクレイピング中断用フラグ
            messagebox.showinfo("スクレイピング", "スクレイピング処理を開始します。しばらくお待ちください。")
            self.gui_init()
            self.scraping_thread = threading.Thread(target=self.scrape_and_create_map)
            self.scraping_thread.start()
 

    def stop_scraping_thread(self):
        if self.scraping_thread and self.scraping_thread.is_alive():
            confirmed = confirmed = messagebox.askquestion("スクレイピング", "スクレイピング処理を中断しますか？")
            
            if confirmed == "yes":
                self.stop_scraping_flg = True

    def scrape_and_create_map(self):
        scraped_data = self.scrape_suumo_data(self.base_url, self.max_page)
        df = pd.DataFrame(scraped_data)
        df = self.preprocess_data(df)
        self.create_map_and_markers(df)
        df.to_csv(CSV_FILE_PATH, encoding="utf-8")

        # 中断処理
        if not self.stop_scraping_flg:
            messagebox.showinfo("スクレイピング完了", "スクレイピングが完了しました。")
        else:
            messagebox.showinfo("スクレイピング中止", "スクレイピングが中止されました。")

        self.scraping_thread = None  # 処理完了のためスレッド初期化

        # 発表用に自動でHTMLを開けるようにしておく
        webbrowser.open(HTML_FILE_PATH)  # HTMLファイルをデフォルトのWebブラウザで開く   
        
    def update_progressbar(self, value):
        self.progress["value"] = value * 10 # 小数点第一位をバーに反映するため
        self.root.update_idletasks()  # GUIの更新

    ### Scraping ###
    # HTML解析
    @retry(tries=3, delay=10, backoff=2)
    def get_html(self, url):
        r = requests.get(url)
        soup = BeautifulSoup(r.content, "html.parser")
        return soup

    #文字列から数字を抽出
    def get_number(self, value):
        n = re.findall(r"[0-9.]+", value)
        if len(n)!=0:
            return float(n[0])
        else:
            return 0
    
    #全角英数を半角英数に変換
    def zenkaku_to_hankaku(self, text):
        return re.sub(r'[Ａ-Ｚａ-ｚ０-９]', lambda x: chr(ord(x.group(0)) - 0xFEE0), text)

    # スクレイピング関数
    def scrape_suumo_data(self, base_url, max_page):
        all_data = []
        for page in range(1, max_page+1):
            
            # 中断処理
            if self.stop_scraping_flg:
                break

            url = base_url.format(page)
            soup = self.get_html(url)
            items = soup.findAll("div", {"class": "cassetteitem"})
            print("page", page, "items", len(items))
            for item in items:
                stations = item.findAll("div", {"class": "cassetteitem_detail-text"})
                for station in stations:
                    base_data = {}
                    base_data["名称"] = item.find("div", {"class": "cassetteitem_content-title"}).getText().strip()
                    base_data["カテゴリー"] = item.find("div", {"class": "cassetteitem_content-label"}).getText().strip()
                    base_data["アドレス"] = item.find("li", {"class": "cassetteitem_detail-col1"}).getText().strip()
                    base_data["アクセス"] = station.getText().strip()
                    base_data["築年数"] = item.find("li", {"class": "cassetteitem_detail-col3"}).findAll("div")[0].getText().strip()
                    base_data["構造"] = item.find("li", {"class": "cassetteitem_detail-col3"}).findAll("div")[1].getText().strip()
                    if item.find('img', class_="js-noContextMenu") is not None:
                        base_data["画像"] = item.find('img', class_="js-noContextMenu").get('rel')
                    else:
                        base_data["画像"] = None
                    tbodys = item.find("table", {"class": "cassetteitem_other"}).findAll("tbody")
                    for tbody in tbodys:
                        data = base_data.copy()
                        data["階数"] = tbody.findAll("td")[2].getText().strip()
                        data["家賃"] = tbody.findAll("td")[3].findAll("li")[0].getText().strip()
                        data["管理費"] = tbody.findAll("td")[3].findAll("li")[1].getText().strip()
                        data["敷金"] = tbody.findAll("td")[4].findAll("li")[0].getText().strip()
                        data["礼金"] = tbody.findAll("td")[4].findAll("li")[1].getText().strip()
                        data["間取り"] = tbody.findAll("td")[5].findAll("li")[0].getText().strip()
                        data["面積"] = tbody.findAll("td")[5].findAll("li")[1].getText().strip()
                        data["URL"] = "https://suumo.jp" + tbody.findAll("td")[8].find("a").get("href")
                        all_data.append(data)
            time.sleep(1)
        return all_data

    # データの成型、重複部分の削除
    def preprocess_data(self, df):
        df["家賃"] = df["家賃"].apply(self.get_number)
        df["管理費"] = df["管理費"].apply(self.get_number)
        df["管理費"] = df["管理費"] / 10000
        df["敷金"] = df["敷金"].apply(self.get_number)
        df["礼金"] = df["礼金"].apply(self.get_number)
        df["面積"] = df["面積"].apply(self.get_number)
        df["築年数"] = df["築年数"].apply(self.get_number)
        df = df[df["アクセス"].notnull()]
        df.drop_duplicates(subset=["名称"], inplace=True)
        df.drop_duplicates(subset=["アドレス", "カテゴリー", "家賃", "敷金", "構造", "礼金", "管理費", "築年数", "間取り", "階数", "面積"], inplace=True)
        df["アドレス"]=df["アドレス"].apply(self.zenkaku_to_hankaku)
        df["名称"]=df["名称"].apply(self.zenkaku_to_hankaku)
        return df


    ### Map ###
    # 緯度経度取得
    def get_location_info(self, address):
        url = KOKUDO_API + address
        try:
            # 国土地理院APIで緯度経度取得
            r = requests.get(url).json()
            if len(r) > 0:
                longitude = r[0]['geometry']['coordinates'][0]  #経度
                latitude = r[0]['geometry']['coordinates'][1]   #緯度
            else:
                # 国土地理院APIで戻ってこない住所があるための対策
                url = ZIPCODA_API + address
                r = requests.get(url)
                postal = str(r.json()['items'][0]['zipcode']) #郵便番号
                res_dict = requests.get(HRG_API + postal).json()['response']['location'][0]
                longitude = res_dict['x']   #経度
                latitude = res_dict['y']    #緯度
            return longitude, latitude
        except Exception as e:
            print(f"Error fetching location info for address '{address},{url}': {e}")
            return None, None
        
    #マップ生成、マーカー生成
    def create_map_and_markers(self, df):
        count = 0
        m = folium.Map(location=[33.5903, 130.4017], zoom_start=13)
        marker_cluster = MarkerCluster().add_to(m)
        added_list = []
        for index, row in df.iterrows():
            # 中断処理
            if self.stop_scraping_flg:
                break

            address = row["アドレス"]
            name = row["名称"]
            suumo_url = row["URL"]
            icon_url = row["画像"]
            rent = row["家賃"]
            house_layout = row["間取り"]
            house_area = row["面積"]
            house_age = row["築年数"]
            longitude, latitude = self.get_location_info(address)
            if longitude is not None and latitude is not None:
                added_list.append((longitude, latitude))
                for coord in added_list:
                    if added_list.count(coord) > 1:
                        cnt = added_list.count(coord)
                        longitude += random.uniform(-0.0001, 0.0001) * cnt
                        latitude += random.uniform(-0.0001, 0.0001) * cnt
                        break
                content_url = f'<a href="{suumo_url}">{suumo_url}</a>'
                popup_content = self.get_marker_popup_content(name, address, rent, house_layout, house_area, house_age, content_url, icon_url)
                folium.Marker([latitude, longitude], tooltip=name, popup=popup_content).add_to(marker_cluster)
                time.sleep(1)
            else:
                print(name, suumo_url)

            print(f"追加完了:{name}")
            count += 1
            complete_ratio = round(count/len(df)*100,3)
            self.update_progressbar(complete_ratio)

            # 現在の件数と総件数の更新
            self.current_count_text.set(f"処理完了: {count}")
            self.total_count_text.set(f"総件数: {len(df)}")

        m.save("fukuoka.html")

    # ポップアップ表示情報生成
    def get_marker_popup_content(self, name, address, rent, house_layout, house_area, house_age, content_url, icon_url):
        property_info = {
            "名称": name,
            "住所": address,
            "賃料": f"{rent}万円",
            "間取り": house_layout,
            "面積": f"{house_area}㎡",
            "築年数": f"{int(house_age)}年",
            "リンク": content_url,
        }
        popup_content = "<br>".join([f"<b>{key}:</b> {value}" for key, value in property_info.items()])
        popup_content += f'<br><img src="{icon_url}" style="max-width:200px;">'
        return popup_content

### main ###
if __name__ == "__main__":
    base_url = SUUMO_URL
    max_page = MAX_PAGE
    app = ScraperGUI(base_url, max_page)
