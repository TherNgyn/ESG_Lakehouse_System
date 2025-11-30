# filepath: d:\Nam4-HK1\TLCN\ESG_Lakehouse_System\utils\config_loader.py

import yaml
from typing import List, Dict

class ConfigLoader:
    
    def __init__(self, patterns_file: str):
        with open(patterns_file, 'r') as f:
            self.patterns_config = yaml.safe_load(f)
    
    def load_all_companies(self) -> List[Dict]:
        """Load configs cho tất cả companies"""
        result = []
        
        for company in self.patterns_config['companies']:
            # Load company-specific config từ file
            with open(company['config_file'], 'r') as f:
                company_config = yaml.safe_load(f)
            
            # Merge: lấy sheets từ company_patterns, excel config từ file riêng
            merged = {
                'id': company['id'],
                'name': company['name'],
                's3_pattern': company['s3_pattern'],
                'sheets': company.get('sheets', []),  # <-- LẤY TỪ PATTERNS
                **company_config  # <-- MERGE VỚI FILE RIÊNG
            }
            
            result.append(merged)
        
        return result