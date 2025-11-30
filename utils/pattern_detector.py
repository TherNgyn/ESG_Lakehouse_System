import re
from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, last, coalesce
from pyspark.sql.window import Window

class ExcelStructureDetector:
    
    @staticmethod
    def detect_year_columns(df):
        """Detect columns that contain year values (2020, 2021, etc.)"""
        year_pattern = r'^(19|20)\d{2}(\.0+)?$'  # Match 1900-2099
        
        year_cols = []
        for col_name in df.columns:
            # Convert to string để match pattern
            col_str = str(col_name).strip()
            if re.match(year_pattern, col_str):
                year_cols.append(col_name)  # Giữ nguyên tên cột gốc
        
        print(f"    >> detect_year_columns found: {year_cols}")
        return sorted(year_cols)
    
    @staticmethod
    def extract_baseline_year(column_name: str, patterns: List[str]) -> Optional[int]:
        """Extract baseline year từ column name"""
        for pattern in patterns:
            match = re.search(pattern, column_name)
            if match:
                return int(match.group(1))
        return None
    
    @staticmethod
    def fill_merged_cells(df: DataFrame, column_name: str) -> DataFrame:
        """
        Fill forward các merged cells (null values trong Excel merged cells)
        VD: "Climate change" ở row đầu, các row sau là null
        """
        window_spec = Window.orderBy(lit(1)).rowsBetween(Window.unboundedPreceding, 0)
        
        return df.withColumn(
            column_name,
            last(col(column_name), ignorenulls=True).over(window_spec)
        )
    
    @staticmethod
    def detect_category_column(df: DataFrame) -> Optional[str]:
        """
        Tìm column chứa category/group (thường là column đầu tiên hoặc có merged cells)
        """
        # Check các column candidates
        candidates = []
        for col_name in df.columns:
            if any(keyword in col_name.lower() for keyword in ['category', 'group', 'section', 'topic']):
                candidates.append(col_name)
        return candidates[0] if candidates else df.columns[0]
    
    @staticmethod
    def infer_topic(text: str, topic_rules: dict) -> str:
        """Infer E/S/G topic từ metric name hoặc category"""
        if not text:
            return 'Unknown'
        
        text_lower = text.lower()
        for topic, rules in topic_rules.items():
            if any(kw in text_lower for kw in rules['keywords']):
                return topic
        return 'Unknown'
    
    @staticmethod
    def extract_category(text: str, category_patterns: list) -> Optional[str]:
        """Extract category từ metric name"""
        if not text:
            return None
            
        for cp in category_patterns:
            if re.search(cp['pattern'], text, re.IGNORECASE):
                return cp['name']
        return None
    
    @staticmethod
    def detect_header_structure(df: DataFrame) -> Dict[str, str]:
        """
        Detect cấu trúc header của Excel:
        - Category column (merged cells)
        - Indicator column (metric name)
        - Unit column
        - Year columns
        """
        columns = df.columns
        
        structure = {
            'category_column': None,
            'indicator_column': None,
            'unit_column': None,
            'year_columns': []
        }
        
        # Detect unit column
        for col_name in columns:
            if 'unit' in col_name.lower():
                structure['unit_column'] = col_name
                break
        
        # Detect indicator column (thường có từ khóa indicator, metric, description)
        for col_name in columns:
            if any(kw in col_name.lower() for kw in ['indicator', 'metric', 'description', 'parameter']):
                structure['indicator_column'] = col_name
                break
        
        # Nếu không tìm thấy, assume column đầu tiên sau category là indicator
        if not structure['indicator_column']:
            # Skip first column (category) và unit column
            for col_name in columns:
                if col_name != structure['category_column'] and col_name != structure['unit_column']:
                    if not re.match(r'^(19|20)\d{2}$', col_name):
                        structure['indicator_column'] = col_name
                        break
        
        # Detect year columns
        structure['year_columns'] = ExcelStructureDetector.detect_year_columns(df)
        
        return structure