import time
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

class ESGScraper:
    def __init__(self, headless=True, delay=2):
        self.delay = delay
        self.scraped_data = []
        self.errors = []
        
        self.companies = [
            'Abbott Laboratories', 'Accenture', 'ACS', 'AEON', 'Air Liquide',
       'Airbus', 'Albertsons', 'Alibaba Group Holding', 'Allstate',
       'Alphabet', 'Aluminum Corp of China', 'Amazon',
       'Amer International Group', 'America Movil', 'American Express',
       'American International Group', 'Anglo American',
       'AnheuserBusch InBev', 'Anhui Conch Group', 'Ansteel Group',
       'Apple', 'Archer Daniels Midland', 'Arrow Electronics',
       'Aviation Industry Corp of China', 'AXA',
       'Banco Bilbao Vizcaya Argentaria', 'Banco Bradesco',
       'Banco do Brasil', 'Banco Santander', 'Bank of America',
       'Bank of China', 'Bank of Communications', 'Bank of Nova Scotia',
       'Barclays', 'BASF', 'Bayer', 'Beijing Automotive Group',
       'Beijing Jianlong Heavy Industry Group', 'Berkshire Hathaway',
       'Best Buy', 'BHP Group', 'BNP Paribas', 'Boeing', 'Bouygues', 'BP',
       'BristolMyers Squibb', 'British American Tobacco', 'Broadcom',
       'Brookfield', 'BYD', 'Caixa Econmica Federal',
       'Canadian Natural Resources', 'Capital One Financial',
       'Cardinal Health', 'CarMax', 'Carrefour', 'Caterpillar', 'Cencora',
       'Cenovus Energy', 'Centene', 'Charter Communications',
       'Chengdu Xingcheng Investment Group', 'Cheniere Energy',
       'China Aerospace Science  Industry',
       'China Communications Construction', 'China Construction Bank',
       'China Electronics Technology Group', 'China Electronics',
       'China Energy Engineering Group', 'China Energy Investment',
       'China FAW Group', 'China Huadian', 'China Huaneng Group',
       'China Life Insurance', 'China Merchants Bank',
       'China Merchants Group', 'China Minmetals',
       'China Minsheng Banking', 'China Mobile Communications',
       'China National Aviation Fuel Group',
       'China National Building Material Group',
       'China National Coal Group', 'China National Nuclear',
       'China National Petroleum', 'China North Industries Group',
       'China Pacific Insurance Group', 'China Poly Group',
       'China Post Group', 'China Railway Engineering Group',
       'China Resources', 'China South Industries Group',
       'China Southern Power Grid',
       'China State Construction Engineering', 'China State Shipbuilding',
       'China Taiping Insurance Group',
       'China United Network Communications', 'China Vanke',
       'Christian Dior', 'CHS', 'Cigna', 'Cisco Systems Inc',
       'CITIC Group', 'Citigroup', 'CJ Corp', 'CK Hutchison Holdings',
       'CocaCola', 'COFCO', 'Comcast', 'Compal Electronics',
       'Compass Group', 'ConocoPhillips',
       'Contemporary Amperex Technology', 'Continental', 'Coop Group',
       'COSCO Shipping', 'Costco Wholesale', 'Country Garden Holdings',
       'CPC', 'Crdit Agricole', 'CRH', 'CRRC Group', 'CVS Health',
       'DR Horton', 'Daiichi Life Holdings', 'Daimler Truck Holding',
       'Daiwa House Industry', 'Danaher', 'Deere', 'Dell Technologies',
       'Denso', 'Deutsche Bahn', 'Deutsche Telekom', 'DHL Group',
       'Dollar General', 'Dongfeng Motor', 'Dow', 'DSV', 'EON',
       'Ecopetrol', 'Edeka Zentrale', 'Electricit de France',
       'Elevance Health', 'ELO Group', 'Enbridge', 'ENEOS Holdings',
       'Energi Danmark Group', 'Energie BadenWurttemberg',
       'Energy Transfer', 'Engie', 'ENI', 'Enterprise Products Partners',
       'Equinor', 'EXOR Group', 'Exxon Mobil', 'Fannie Mae', 'FedEx',
       'Fomento Econmico Mexicano', 'Ford Motor', 'Freddie Mac',
       'GasTerra', 'Gazprom', 'General Dynamics', 'General Electric',
       'General Motors', 'Glencore', 'Goldman Sachs Group', 'Google',
       'Greenland Holding Group', 'Groupe BPCE', 'GS Caltex', 'GSK',
       'Guangxi Investment Group', 'Guangzhou Automobile Industry Group',
       'Guangzhou Industrial Investment Holdings',
       'Guangzhou Municipal Construction Group',
       'Guangzhou Pharmaceutical Holdings', 'Haier Smart Home',
       'Hangzhou Iron and Steel Group', 'Hanwha', 'HapagLloyd',
       'HBIS Group', 'HD Hyundai', 'Hengli Group', 'HF Sinclair',
       'Hitachi', 'Home Depot', 'Hon Hai Precision Industry',
       'Honeywell International', 'HP', 'HSBC Holdings',
       'Huawei Investment  Holding', 'Humana', 'Hunan Iron  Steel Group',
       'Hyundai Mobis', 'Hyundai Motor', 'Iberdrola', 'IBM',
       'Idemitsu Kosan', 'Indian Oil',
       'Industrial  Commercial Bank of China', 'Industrial Bank',
       'ING Group', 'Ingka Group', 'Intel', 'Intesa Sanpaolo',
       'Ita Unibanco Holding', 'J Sainsbury', 'Jabil', 'Jardine Matheson',
       'JBS', 'JDcom', 'Jiangsu Shagang Group', 'Jiangxi Copper',
       'Jinchuan Group', 'Jingye Group', 'JPMorgan Chase',
       'KB Financial Group', 'KDDI', 'Ko Holding', 'Korea Electric Power',
       'Korea Gas', 'Kroger', 'La Poste', 'Lennar', 'Lenovo', 'LG Chem',
       'LG Electronics', 'Life Insurance Corp of India',
       'Lockheed Martin', 'Longfor Group Holdings', 'LOreal',
       'Louis Dreyfus', 'Luan Chemical Group', 'Lufthansa Group',
       'Luxshare Precision Industry', 'LyondellBasell Industries',
       'Maersk Group', 'Magnit', 'Marathon Petroleum', 'Marubeni',
       'Massachusetts Mutual Life', 'Medtronic',
       'Meiji Yasuda Life Insurance', 'MercedesBenz Group', 'Merck US',
       'Meta Platforms', 'MetLife', 'Metro', 'Microsoft',
       'Mitsubishi Heavy Industries', 'Mitsubishi UFJ Financial Group',
       'Mitsui', 'Mizuho Financial Group', 'Molina Healthcare',
       'Mondelez International', 'Morgan Stanley',
       'MSAD Insurance Group Holdings', 'Munich Re Group', 'Nationwide',
       'Nestl', 'Netflix', 'New China Life Insurance',
       'New Hope Holding Group', 'New York Life Insurance', 'Nike',
       'Nippon Life Insurance', 'Nissan Motor', 'Northrop Grumman',
       'Northwestern Mutual', 'Novartis', 'NRG Energy',
       'NTT Nippon Telegraph  Telephone', 'Nucor', 'Nutrien',
       'Occidental Petroleum', 'Oil  Natural Gas', 'Olam Group',
       'OMV Group', 'Oracle', 'Orange', 'Pacific Construction Group',
       'Panasonic Holdings', 'Paramount Global', 'PBF Energy', 'Pemex',
       'Peoples Insurance Co of China', 'Performance Food Group',
       'Pertamina', 'Petrobras', 'Petronas', 'Pfizer',
       'Philip Morris International', 'Phillips 66', 'Phoenix Pharma',
       'Ping An Insurance', 'Plains GP Holdings', 'POSCO Holdings',
       'Power Corp of Canada', 'PowerChina', 'Procter  Gamble',
       'Progressive', 'Prudential Financial US', 'PTT',
       'Publix Super Markets', 'Qualcomm', 'Quanta Computer', 'Raizen',
       'Rajesh Exports', 'Reliance Industries', 'Repsol',
       'Rio Tinto Group', 'Roche Group', 'RWE', 'SF Holding',
       'SAIC Motor', 'SaintGobain', 'Salesforce', 'Samsung CT',
       'Samsung Life Insurance', 'Sanofi', 'SAP', 'Saudi Aramco',
       'Sberbank', 'Seven  I Holdings', 'Shaanxi Coal  Chemical Industry',
       'Shaanxi Construction Engineering Holding',
       'Shaanxi Yanchang Petroleum Group', 'Shandong Energy Group',
       'Shandong HiSpeed Group', 'Shandong Weiqiao Pioneering Group',
       'Shanghai Delong Steel Group', 'Shanghai Pharmaceuticals Holding',
       'Shanghai Pudong Development Bank', 'Shanxi Coking Coal Group',
       'Shell', 'Shenghong Holding Group', 'Shenzhen Investment Holdings',
       'Shougang Group', 'Shudao Investment Group', 'Siemens Energy',
       'Siemens', 'Sinochem Holdings', 'Sinomach', 'SK Hynix', 'SK',
       'Socit Gnrale', 'Sompo Holdings', 'State Bank of India',
       'State Farm Insurance', 'State Power Investment',
       'Sumitomo Life Insurance', 'Sumitomo Mitsui Financial Group',
       'Sumitomo', 'Suncor Energy', 'Susun Construction Group',
       'Suzuki Motor', 'Swiss Re', 'Sysco', 'Taikang Insurance Group',
       'TD Synnex', 'Telefonica', 'Tencent Holdings', 'Tesco', 'Tesla',
       'ThyssenKrupp', 'TIAA', 'TJX', 'Tokio Marine Holdings',
       'Tokyo Electric Power', 'TongLing Nonferrous Metals Group',
       'TotalEnergies', 'Toyota Motor', 'Toyota Tsusho',
       'Trafigura Group', 'Trafigura', 'Travelers Cos',
       'Tsingshan Holding Group', 'Tyson Foods', 'US Postal Service',
       'Unilever', 'Uniper', 'UnitedHealth Group', 'UPS', 'USAA',
       'Valero Energy', 'Vale', 'Veolia Environnement',
       'Verizon Communications', 'Vibra Energia', 'Vodafone Group',
       'Volkswagen', 'Volkswagen AG', 'Volvo', 'Walgreens Boots Alliance',
       'Walmart', 'Walt Disney', 'Wells Fargo', 'Wilmar International',
       'Woolworths Group', 'World Kinect', 'Wuchan Zhongda Group',
       'X5 Retail Group', 'Xiamen CD', 'Xiamen ITG Holding Group',
       'Xiaomi', 'Xinjiang Guanghui Industry Investment',
       'Xinjiang Zhongtai Group', 'XMXYG', 'ZF Friedrichshafen',
       'Zhejiang Communications Investment Group',
       'Zhejiang Hengyi Group', 'Zijin Mining Group'
        ]
        
        self.init_driver(headless)
    
    def init_driver(self, headless):
        options = Options()
        if headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
        except:
            self.driver = webdriver.Chrome(options=options)
        
        self.driver.set_page_load_timeout(30)
        self.wait = WebDriverWait(self.driver, 15)
    
    def get_suggestions(self, company_name):
        try:
            self.driver.get("https://www.spglobal.com/sustainable1/en/scores/results")
            time.sleep(1.5)

            search_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".search-bar-autocomplete-component_input-field"))
            )
            search_input.clear()
            search_input.send_keys(company_name)
            time.sleep(2)

            suggestions = self.driver.find_elements(By.CSS_SELECTOR, 
                ".search-bar-autocomplete-component_content-list li")
            
            results = []
            for sug in suggestions:
                text = sug.text.strip()
                if text and len(text) > 2:
                    results.append({'text': text, 'element': sug})
            
            print(f"[{company_name}] Found {len(results)} suggestions")
            return results
            
        except Exception as e:
            print(f"[{company_name}] Error getting suggestions: {e}")
            return []
    
    def extract_data(self, company_name, original_search):
        try:
            time.sleep(2)
        
            if "cid=" not in self.driver.current_url:
                return None
            cells = self.driver.find_elements(By.CSS_SELECTOR, ".esg-table-cell")

            if len(cells) >= 12:
                data_cells = cells[6:12] 
                
                return {
                    'company': data_cells[0].text.strip(),
                    'industry': data_cells[1].text.strip(),
                    'csa_score': data_cells[2].text.strip(),
                    'esg_score': data_cells[3].text.strip(),
                    'score_under_review': data_cells[4].text.strip(),
                    'last_updated': data_cells[5].text.strip(),
                    'search_term': original_search,
                    'url': self.driver.current_url,
                    'scraped_at': datetime.now().isoformat()
                }
            
            return None
            
        except Exception as e:
            return None
    
    def scrape_company(self, company_name):
        results = []
        suggestions = self.get_suggestions(company_name)
        
        if not suggestions:
            self.errors.append(company_name)
            return results
        
        for i, sug in enumerate(suggestions, 1):
            try:
                print(f"  [{i}/{len(suggestions)}] {sug['text'][:50]}", end=" ")
                self.driver.execute_script("arguments[0].click();", sug['element'])
                data = self.extract_data(sug['text'], company_name)
                
                if data:
                    results.append(data)
                    print(f"ESG: {data['esg_score']}")
                else:
                    print("No data")
                if i < len(suggestions):
                    suggestions = self.get_suggestions(company_name)
                    if i < len(suggestions):
                        sug = suggestions[i]
                time.sleep(0.5)  
            except Exception as e:
                continue
        
        print(f"[{company_name}]: {len(results)}")
        return results
    
    def scrape_all(self):
        total = len(self.companies)      
        for idx, company in enumerate(self.companies, 1):
            print(f"\n[{idx}/{total}] {company}")
            try:
                results = self.scrape_company(company)
                
                if results:
                    self.scraped_data.extend(results)
                if idx % 10 == 0:
                    self.save_csv(f"checkpoint_{idx}.csv")
                
                time.sleep(self.delay)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.errors.append(f"{company}: {e}")
    
    def save_csv(self, filename='esg_data.csv'):
        if not self.scraped_data:
            print("No data to save")
            return
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.scraped_data[0].keys())
            writer.writeheader()
            writer.writerows(self.scraped_data)
        
        unique = len(set(d['company'] for d in self.scraped_data))
        print(f"Saved: {filename}")
        print(f"Unique companies: {unique}")
    
    def close(self):
        if self.driver:
            self.driver.quit()


def main():
    scraper = ESGScraper(headless=False, delay=2)
    
    try:
        scraper.scrape_all()
        if scraper.scraped_data:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            scraper.save_csv(f'esg_data_{timestamp}.csv')
        
    except KeyboardInterrupt:
        if scraper.scraped_data:
            scraper.save_csv('esg_partial.csv')
    
    finally:
        scraper.close()


if __name__ == '__main__':
    main()