import streamlit as st
import sys
sys.path.append('.')
from data_loader import DataLoader
from ui_components import render_header, render_footer

from page_modules import home, company_detail, about, contact, methodology

st.set_page_config(
    page_title="ESG Lakehouse Analytics",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    :root {
        --primary-green: #93BD57;
        --bg-cream: #FFFDE1;
        --accent-yellow: #FBE580;
        --accent-red: #980404;
        --text-dark: #2C2C2C;
    }
    
    .stApp {
        background-color: white;
    }
    
    .main-header {
        background: var(--bg-cream);
        padding: 1rem 2rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 2rem;
    }
    
    .logo-text {
        font-size: 1.8rem;
        font-weight: 700;
        color: var(--primary-green);
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .nav-link {
        color: var(--text-dark);
        text-decoration: none;
        padding: 0.5rem 1rem;
        border-radius: 4px;
        transition: background 0.3s;
    }
    
    .nav-link:hover {
        background: var(--accent-yellow);
    }
    
    .nav-link.active {
        background: var(--primary-green);
        color: white;
    }
    
    .company-card {
        background: white;
        border-radius: 8px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        transition: transform 0.2s, box-shadow 0.2s;
    }
    
    .company-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.12);
    }
    
    .score-badge {
        display: inline-block;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.9rem;
    }
    
    .score-high {
        background: #D4EDDA;
        color: #155724;
    }
    
    .score-medium {
        background: var(--accent-yellow);
        color: #856404;
    }
    
    .score-low {
        background: #F8D7DA;
        color: var(--accent-red);
    }
    
    .filter-container {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    
    .metric-card {
        background: linear-gradient(135deg, var(--primary-green), #7BA850);
        color: white;
        padding: 1.5rem;
        border-radius: 8px;
        text-align: center;
    }
    
    .stButton>button {
        background: var(--primary-green);
        color: white;
        border: none;
        border-radius: 4px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: background 0.3s;
    }
    
    .stButton>button:hover {
        background: #7BA850;
    }
    
    .footer {
        background: var(--bg-cream);
        padding: 2rem;
        margin-top: 3rem;
        border-top: 2px solid var(--primary-green);
    }
    
    div[data-testid="stDataFrame"] {
        background: white;
        border-radius: 8px;
        padding: 1rem;
    }
    
    .small-logo {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        object-fit: contain;
        background: white;
        padding: 4px;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_data_loader():
    return DataLoader()

def main():
    loader = get_data_loader()
    
    render_header()
    
    if 'page' not in st.session_state:
        st.session_state.page = 'home'
    
    if 'selected_company' in st.session_state and st.session_state.selected_company:
        company_detail.show(loader, st.session_state.selected_company)
    elif st.session_state.page == 'home':
        home.show(loader)
    elif st.session_state.page == 'about':
        about.show()
    elif st.session_state.page == 'contact':
        contact.show()
    elif st.session_state.page == 'methodology':
        methodology.show()
    
    render_footer()

if __name__ == "__main__":
    main()