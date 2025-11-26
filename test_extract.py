import os
from pathlib import Path
from unstructured.partition.pdf import partition_pdf
from unstructured.documents.elements import Table
import subprocess
import pandas as pd
import tabula
import re
from datetime import datetime
import pdfplumber
import json

class PDFTableExtractor:
    def __init__(self, base_dir=None):
        if base_dir is None:
            self.current_dir = Path(__file__).resolve().parent
            self.base_dir = self.current_dir.parent
        else:
            self.base_dir = Path(base_dir)
            
        self.setup_environment()
        self.output_dir = self.base_dir / "data" / "bronze" / "pdf_tables"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def setup_environment(self):
        """Setup Poppler & Tesseract environment"""
        poppler_path = self.base_dir / "requirement" / "poppler-25.07.0" / "Library" / "bin"
        tesseract_path = self.base_dir / "requirement" / "Tesseract-OCR"
        tessdata_path = tesseract_path / "tessdata"
        
        if poppler_path.exists():
            os.environ["PATH"] += os.pathsep + str(poppler_path)
        if tesseract_path.exists():
            os.environ["PATH"] += os.pathsep + str(tesseract_path)
        if tessdata_path.exists():
            os.environ["TESSDATA_PREFIX"] = str(tessdata_path)
        
        print("Environment setup completed")
    
    def is_wide_table(self, df):
        """
        Phát hiện bảng ngang với logic cải tiến:
        - Bảng có >= 4 cột
        - Cột đầu tiên là text labels
        - Các cột còn lại chủ yếu là số
        """
        if df.empty or len(df.columns) < 4:
            return False, {}
        
        analysis = {
            'num_columns': len(df.columns),
            'num_rows': len(df),
            'col_row_ratio': len(df.columns) / len(df) if len(df) > 0 else 0
        }
        
        # Điều kiện 1: Số cột >= 4 (relax hơn để catch nhiều case)
        has_enough_cols = analysis['num_columns'] >= 4
        
        # Điều kiện 2: Phân tích cột đầu tiên (phải là text)
        first_col_is_text = False
        first_col_avg_length = 0
        try:
            first_col = df.iloc[:, 0].astype(str)
            first_col_avg_length = first_col.str.len().mean()
            
            # Đếm cells chứa text (không phải số thuần túy)
            text_cells = sum(1 for val in first_col if not str(val).replace('.', '').replace(',', '').replace(' ', '').isdigit())
            first_col_is_text = text_cells / len(first_col) >= 0.7  # 70% là text
            
            analysis['first_col_avg_length'] = float(first_col_avg_length)
            analysis['first_col_text_ratio'] = float(text_cells / len(first_col))
        except:
            pass
        
        # Điều kiện 3: Các cột còn lại chủ yếu là số
        numeric_cols = 0
        total_other_cols = len(df.columns) - 1
        
        for col_idx in range(1, len(df.columns)):
            col = df.iloc[:, col_idx]
            try:
                # Đếm cells có thể convert sang số
                col_str = col.astype(str).str.replace(',', '').str.replace('.', '', 1).str.strip()
                numeric_count = sum(1 for val in col_str if val.replace('.', '').replace('-', '').isdigit())
                
                if numeric_count / len(col) >= 0.5:  # 50% cells là số
                    numeric_cols += 1
            except:
                pass
        
        numeric_ratio = numeric_cols / total_other_cols if total_other_cols > 0 else 0
        analysis['numeric_columns'] = int(numeric_cols)
        analysis['numeric_ratio'] = float(numeric_ratio)
        
        has_numeric_data = numeric_ratio >= 0.5  # Relax: 50% cột là số
        
        # Điều kiện 4: Header detection (row đầu thường ngắn hơn)
        has_clear_header = False
        try:
            if len(df) >= 2:
                first_row_len = df.iloc[0].astype(str).str.len().mean()
                rest_rows_len = df.iloc[1:].astype(str).apply(lambda x: x.str.len().mean(), axis=1).mean()
                has_clear_header = first_row_len < rest_rows_len * 0.8  # Header ngắn hơn 80%
                analysis['header_length_ratio'] = float(first_row_len / rest_rows_len if rest_rows_len > 0 else 1)
        except:
            pass
        
        # QUYẾT ĐỊNH: Wide table
        # Case 1: Đủ cột + cột đầu là text + phần lớn cột khác là số
        is_wide = (has_enough_cols and first_col_is_text and has_numeric_data)
        
        # Case 2: Hoặc đủ cột + tỷ lệ cột/hàng cao (bảng rộng)
        if not is_wide:
            is_wide = has_enough_cols and analysis['col_row_ratio'] >= 0.4 and first_col_is_text
        
        analysis['is_wide_table'] = bool(is_wide)
        analysis['reasons'] = {
            'has_enough_cols': bool(has_enough_cols),
            'first_col_is_text': bool(first_col_is_text),
            'has_numeric_data': bool(has_numeric_data),
            'has_clear_header': bool(has_clear_header)
        }
        
        return is_wide, analysis
    
    def detect_table_structure(self, df):
        """Phát hiện cấu trúc bảng chi tiết"""
        structure = {
            'type': 'unknown',
            'has_header': False,
            'has_subtotals': False,
            'has_merged_cells': False,
            'orientation': 'vertical'
        }
        
        if df.empty:
            return structure
        
        # Detect orientation
        is_wide, wide_analysis = self.is_wide_table(df)
        if is_wide:
            structure['orientation'] = 'horizontal'
            structure['type'] = 'wide_table'
            structure['wide_analysis'] = wide_analysis
        
        # Detect header
        try:
            first_row = df.iloc[0].astype(str)
            non_numeric = sum(not str(val).replace('.', '').replace(',', '').replace('-', '').isdigit() 
                            for val in first_row)
            if non_numeric >= len(df.columns) * 0.6:
                structure['has_header'] = True
        except:
            pass
        
        # Detect subtotals
        try:
            content = df.astype(str).values.flatten()
            content_text = ' '.join(content).lower()
            if any(keyword in content_text for keyword in ['tổng', 'total', 'cộng', 'sum', 'subtotal']):
                structure['has_subtotals'] = True
        except:
            pass
        
        return structure
    
    def method_3_pdfplumber(self, pdf_path):
        """Method 3: PDFPlumber với multiple strategies - ENHANCED"""
        print("\n=== METHOD 3: PDFPLUMBER (MULTI-STRATEGY ENHANCED) ===")
        
        try:
            tables = []
            with pdfplumber.open(pdf_path) as pdf:
                for page_number, page in enumerate(pdf.pages, start=1):
                    print(f"\nProcessing page {page_number}...")
                    
                    all_extracted = []
                    
                    # Strategy 1: TEXT-BASED với aggressive settings (cho bảng có header màu)
                    try:
                        text_settings = {
                            "vertical_strategy": "text",
                            "horizontal_strategy": "text",  # Dùng text cho cả 2 chiều
                            "snap_tolerance": 8,  # Tăng tolerance
                            "join_tolerance": 8,
                            "edge_min_length": 3,
                            "min_words_vertical": 1,  # Giảm threshold
                            "min_words_horizontal": 1,
                            "intersection_tolerance": 8,
                            "text_x_tolerance": 5,  # Thêm tolerance cho text alignment
                            "text_y_tolerance": 5
                        }
                        tables_text = page.extract_tables(table_settings=text_settings)
                        if tables_text:
                            all_extracted.extend([('text_aggressive', t) for t in tables_text])
                            print(f"  Strategy TEXT-AGGRESSIVE: {len(tables_text)} tables")
                    except Exception as e:
                        print(f"  Strategy TEXT-AGGRESSIVE failed: {str(e)}")
                    
                    # Strategy 2: TEXT với standard settings
                    try:
                        text_settings = {
                            "vertical_strategy": "text",
                            "horizontal_strategy": "lines",
                            "snap_tolerance": 5,
                            "join_tolerance": 5,
                            "edge_min_length": 5,
                            "min_words_vertical": 2,
                            "min_words_horizontal": 1,
                            "intersection_tolerance": 5
                        }
                        tables_text = page.extract_tables(table_settings=text_settings)
                        if tables_text:
                            for t in tables_text:
                                if not any(self._tables_similar(t, existing[1]) for existing in all_extracted):
                                    all_extracted.append(('text', t))
                            print(f"  Strategy TEXT: {len(tables_text)} tables")
                    except Exception as e:
                        print(f"  Strategy TEXT failed: {str(e)}")
                    
                    # Strategy 3: LINES (cho bảng có đầy đủ borders)
                    try:
                        lines_settings = {
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "snap_tolerance": 3,
                            "join_tolerance": 3
                        }
                        tables_lines = page.extract_tables(table_settings=lines_settings)
                        if tables_lines:
                            for t in tables_lines:
                                if not any(self._tables_similar(t, existing[1]) for existing in all_extracted):
                                    all_extracted.append(('lines', t))
                            print(f"  Strategy LINES: {len(tables_lines)} tables")
                    except Exception as e:
                        print(f"  Strategy LINES failed: {str(e)}")
                    
                    # Strategy 4: EXPLICIT với low thresholds (cho bảng phức tạp)
                    try:
                        explicit_settings = {
                            "vertical_strategy": "explicit",
                            "horizontal_strategy": "explicit",
                            "explicit_vertical_lines": page.curves + page.edges,
                            "explicit_horizontal_lines": page.curves + page.edges,
                            "snap_tolerance": 10,
                            "join_tolerance": 10
                        }
                        tables_explicit = page.extract_tables(table_settings=explicit_settings)
                        if tables_explicit:
                            for t in tables_explicit:
                                if not any(self._tables_similar(t, existing[1]) for existing in all_extracted):
                                    all_extracted.append(('explicit', t))
                            print(f"  Strategy EXPLICIT: {len(tables_explicit)} tables")
                    except Exception as e:
                        print(f"  Strategy EXPLICIT failed: {str(e)}")
                    
                    # Strategy 5: DEFAULT (fallback)
                    if not all_extracted:
                        try:
                            tables_default = page.extract_tables()
                            if tables_default:
                                all_extracted.extend([('default', t) for t in tables_default])
                                print(f"  Strategy DEFAULT: {len(tables_default)} tables")
                        except Exception as e:
                            print(f"  Strategy DEFAULT failed: {str(e)}")
                    
                    extracted_tables = [t for strategy, t in all_extracted]
                    print(f"  Total unique tables: {len(extracted_tables)}")
                    
                    for i, table in enumerate(extracted_tables):
                        if not table or len(table) < 2:
                            continue
                        
                        try:
                            # Kiểm tra consistency
                            col_lengths = [len(row) for row in table if row]
                            if not col_lengths:
                                continue
                            
                            max_cols = max(col_lengths)
                            
                            # Pad rows thiếu cột
                            normalized_table = []
                            for row in table:
                                if row:
                                    padded_row = row + [None] * (max_cols - len(row))
                                    normalized_table.append(padded_row[:max_cols])
                            
                            if len(normalized_table) < 2:
                                continue
                            
                            # Tạo DataFrame
                            headers = normalized_table[0]
                            data_rows = normalized_table[1:]
                            
                            # Clean headers - xử lý đặc biệt cho headers có thể bị thiếu
                            cleaned_headers = []
                            for j, h in enumerate(headers):
                                if h and str(h).strip():
                                    cleaned_headers.append(str(h).strip())
                                else:
                                    # Nếu header rỗng, thử lấy từ row đầu tiên của data
                                    if data_rows and j < len(data_rows[0]) and data_rows[0][j]:
                                        cleaned_headers.append(str(data_rows[0][j]).strip())
                                    else:
                                        cleaned_headers.append(f"Col_{j}")
                            
                            headers = cleaned_headers
                            
                            df = pd.DataFrame(data_rows, columns=headers)
                            df_clean = self._clean_dataframe(df)
                            
                            if df_clean.empty or df_clean.shape[0] == 0:
                                print(f"    Table {i}: Empty after cleaning")
                                continue
                            
                            # Detect structure
                            structure = self.detect_table_structure(df_clean)
                            
                            orientation = "WIDE" if structure['orientation'] == 'horizontal' else "regular"
                            print(f"    Table {i}: {df_clean.shape[0]}x{df_clean.shape[1]} [{orientation}]")
                            
                            if structure['orientation'] == 'horizontal':
                                print(f"      → Wide table analysis:")
                                wa = structure.get('wide_analysis', {})
                                print(f"         - Columns: {wa.get('num_columns')}")
                                print(f"         - Numeric ratio: {wa.get('numeric_ratio', 0):.2%}")
                                print(f"         - First col text: {wa.get('first_col_is_text', False)}")
                            
                            tables.append({
                                'method': 'pdfplumber',
                                'table_id': f"pdfplumber_p{page_number}_t{i}",
                                'dataframe': df_clean,
                                'shape': df_clean.shape,
                                'structure': structure,
                                'page': page_number
                            })
                            
                        except Exception as e:
                            print(f"    Error processing table {i}: {str(e)}")
                            continue
            
            return tables
        
        except Exception as e:
            print(f"PDFPlumber error: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def _tables_similar(self, table1, table2):
        """Check if two tables are similar (deduplicate)"""
        try:
            if not table1 or not table2:
                return False
            if len(table1) != len(table2):
                return False
            if len(table1[0]) != len(table2[0]):
                return False
            
            # Compare first row
            row1 = [str(cell) for cell in table1[0] if cell]
            row2 = [str(cell) for cell in table2[0] if cell]
            
            return row1 == row2
        except:
            return False
    
    def method_2_tabula(self, pdf_path):
        """Method 2: Tabula với nhiều strategies"""
        print("\n=== METHOD 2: TABULA ===")
        
        try:
            # Try multiple extraction strategies
            strategies = [
                {'lattice': True},   # Cho bảng có border
                {'stream': True, 'guess': False},    # Cho bảng không border
                {'stream': True, 'guess': True},     # Auto-detect aggressive
                {}                   # Auto-detect default
            ]
            
            all_dfs = []
            for idx, strategy in enumerate(strategies):
                try:
                    dfs = tabula.read_pdf(
                        str(pdf_path), 
                        pages='all',
                        multiple_tables=True,
                        pandas_options={'header': None},
                        silent=True,
                        **strategy
                    )
                    if dfs:
                        print(f"  Strategy {idx+1}: {len(dfs)} tables")
                        all_dfs.extend(dfs)
                except Exception as e:
                    print(f"  Strategy {idx+1} failed: {str(e)}")
            
            # Deduplicate
            unique_tables = []
            seen_shapes = set()
            
            for df in all_dfs:
                if not df.empty and df.shape[0] > 1:
                    shape_sig = (df.shape[0], df.shape[1], hash(str(df.iloc[0].tolist())))
                    if shape_sig not in seen_shapes:
                        seen_shapes.add(shape_sig)
                        unique_tables.append(df)
            
            print(f"Found {len(unique_tables)} unique tables")
            
            tables = []
            for i, df in enumerate(unique_tables):
                df_clean = self._clean_dataframe(df)
                if not df_clean.empty:
                    structure = self.detect_table_structure(df_clean)
                    
                    orientation = "WIDE" if structure['orientation'] == 'horizontal' else "regular"
                    print(f"  Table {i}: {df_clean.shape[0]}x{df_clean.shape[1]} [{orientation}]")
                    
                    tables.append({
                        'method': 'tabula',
                        'table_id': f"tabula_t{i}",
                        'dataframe': df_clean,
                        'shape': df_clean.shape,
                        'structure': structure
                    })
            
            return tables
            
        except Exception as e:
            print(f"Tabula error: {str(e)}")
            return []
    
    def _clean_dataframe(self, df):
        """Clean DataFrame với xử lý None/NaN tốt hơn"""
        if df.empty:
            return df
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove completely empty columns
        df = df.dropna(axis=1, how='all')
        
        if df.empty:
            return df
        
        # Reset index
        df = df.reset_index(drop=True)
        
        # Clean cell values
        for col in df.columns:
            try:
                # Convert to string
                df[col] = df[col].astype(str)
                
                # Strip whitespace
                df[col] = df[col].str.strip()
                
                # Replace various null representations
                df[col] = df[col].replace(['nan', 'None', 'NaN', 'null', '', ' '], pd.NA)
                
            except Exception as e:
                print(f"Warning: Could not clean column {col}: {str(e)}")
                pass
        
        # Remove rows where first column is empty (usually not data)
        if len(df.columns) > 0:
            df = df[df.iloc[:, 0].notna()]
        
        return df
    
    def detect_table_type(self, df, structure=None):
        """Detect loại bảng"""
        if df.empty:
            return "unknown"
        
        content = df.astype(str).values.flatten()
        content_text = ' '.join(content).lower()
        
        # Priority 1: Wide table classification
        if structure and structure.get('orientation') == 'horizontal':
            if any(kw in content_text for kw in ['nhân viên', 'lao động', 'người', 'tuổi', 'age', 'employee', 'workforce']):
                return "workforce_wide"
            elif any(kw in content_text for kw in ['quản lý', 'cấp', 'level', 'management', 'quan ly']):
                return "management_wide"
            elif any(kw in content_text for kw in ['đào tạo', 'dao tao', 'training', 'khóa', 'khoa', 'course']):
                return "training_wide"
            else:
                return "data_wide"
        
        # Regular classification
        energy_keywords = ['điện', 'kwh', 'năng lượng', 'biomass', 'evn', 'mj', 'gas', 'coal', 'tiêu thụ']
        financial_keywords = ['tỷ', 'triệu', 'đồng', 'vnd', 'usd', 'revenue', 'profit', 'doanh thu']
        emission_keywords = ['phát thải', 'co2', 'emission', 'khí', 'carbon', 'scope']
        
        if any(kw in content_text for kw in energy_keywords):
            return "energy_consumption"
        elif any(kw in content_text for kw in financial_keywords):
            return "financial"
        elif any(kw in content_text for kw in emission_keywords):
            return "emissions"
        else:
            return "general"
    
    def _serialize_structure(self, structure):
        """Convert structure dict to JSON-serializable format"""
        serializable = {}
        for key, value in structure.items():
            if isinstance(value, dict):
                serializable[key] = self._serialize_structure(value)
            elif isinstance(value, (bool, int, float, str)):
                serializable[key] = value
            elif isinstance(value, (list, tuple)):
                serializable[key] = [self._serialize_value(v) for v in value]
            else:
                serializable[key] = str(value)
        return serializable
    
    def _serialize_value(self, value):
        """Convert individual value to JSON-serializable format"""
        if isinstance(value, (bool, int, float, str)):
            return value
        elif isinstance(value, dict):
            return self._serialize_structure(value)
        elif isinstance(value, (list, tuple)):
            return [self._serialize_value(v) for v in value]
        else:
            return str(value)
    
    def save_tables(self, all_tables):
        """Save tables to CSV with proper JSON serialization"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        for table_info in all_tables:
            table_id = table_info['table_id']
            df = table_info['dataframe']
            method = table_info['method']
            structure = table_info.get('structure', {})
            
            table_type = self.detect_table_type(df, structure)
            orientation = structure.get('orientation', 'vertical')
            
            filename = f"{timestamp}_{method}_{orientation}_{table_type}_{table_id}.csv"
            filepath = self.output_dir / filename
            
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            # Serialize structure properly
            serializable_structure = self._serialize_structure(structure)
            
            metadata = {
                'filename': filename,
                'method': method,
                'table_type': table_type,
                'shape': list(table_info['shape']),
                'structure': serializable_structure,
                'page': int(table_info.get('page', 0)) if table_info.get('page') else None,
                'extracted_at': datetime.now().isoformat()
            }
            
            saved_files.append(metadata)
            
            marker = "🔷 WIDE" if orientation == 'horizontal' else "🔹"
            print(f"✓ {marker} {filename} ({df.shape[0]}x{df.shape[1]})")
        
        # Save summary with proper serialization
        summary_file = self.output_dir / f"{timestamp}_extraction_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(saved_files, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Summary: {summary_file}")
        return saved_files
    
    def extract_all_methods(self, pdf_path):
        """Extract tables using all available methods"""
        print(f"{'='*60}")
        print(f"PDF Table Extractor - Wide Table Detection (Enhanced)")
        print(f"{'='*60}")
        print(f"Target: {pdf_path}")
        print(f"Exists: {pdf_path.exists()}\n")
        
        if not pdf_path.exists():
            print("❌ PDF not found!")
            return []
        
        all_tables = []
        methods_status = {}
        
        # Try Method 3 first (most reliable for complex tables)
        try:
            print("\n" + "="*60)
            tables_3 = self.method_3_pdfplumber(pdf_path)
            all_tables.extend(tables_3)
            methods_status['pdfplumber'] = f"✓ {len(tables_3)} tables"
        except Exception as e:
            methods_status['pdfplumber'] = f"✗ Error"
            print(f"PDFPlumber error: {str(e)}")
        
        # Try Method 2 if needed
        if len(all_tables) == 0:
            try:
                print("\n" + "="*60)
                tables_2 = self.method_2_tabula(pdf_path)
                all_tables.extend(tables_2)
                methods_status['tabula'] = f"✓ {len(tables_2)} tables"
            except Exception as e:
                methods_status['tabula'] = f"✗ Error"
                print(f"Tabula error: {str(e)}")
        
        # Summary
        print(f"\n{'='*60}")
        print(f"EXTRACTION SUMMARY")
        print(f"{'='*60}")
        print(f"\nMethods Status:")
        for method, status in methods_status.items():
            print(f"  {method:15} : {status}")
        
        print(f"\nTotal tables: {len(all_tables)}")
        
        wide_count = sum(1 for t in all_tables 
                        if t.get('structure', {}).get('orientation') == 'horizontal')
        if wide_count > 0:
            print(f"🔷 Wide tables: {wide_count}")
        
        if all_tables:
            print(f"\n{'='*60}")
            print("SAVING TABLES...")
            print(f"{'='*60}\n")
            saved = self.save_tables(all_tables)
            
            print(f"\n{'='*60}")
            print(f"✓ COMPLETED: {len(saved)} tables saved")
            print(f"Location: {self.output_dir}")
            print(f"{'='*60}\n")
            
            return saved
        else:
            print("\n❌ No tables extracted!")
            print("\n💡 Troubleshooting:")
            print("  1. Check if PDF has extractable tables (not scanned images)")
            print("  2. Try opening PDF manually to verify tables exist")
            print("  3. Consider using OCR if tables are in images")
            return []

if __name__ == "__main__":
    extractor = PDFTableExtractor()
    
    pdf_path = extractor.base_dir / "ESG_Lakehouse_System" / "datasets" / "reports" / "vinamilk_2024.pdf"
    
    results = extractor.extract_all_methods(pdf_path)
    
    if results:
        print("\n📊 EXTRACTED TABLES:\n")
        for r in results:
            struct = r.get('structure', {})
            marker = "🔷" if struct.get('orientation') == 'horizontal' else "🔹"
            print(f"  {marker} {r['filename']}")
            print(f"     Type: {r['table_type']} | Shape: {r['shape']}")
            if struct.get('orientation') == 'horizontal':
                wa = struct.get('wide_analysis', {})
                print(f"     Analysis: {wa.get('num_columns')} cols, "
                      f"{wa.get('numeric_ratio', 0):.1%} numeric")
            print()