import streamlit as st
import folium
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

def render_header():
    """Render header with logo and navigation"""
    header_html = """
    <div class="main-header">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div class="logo-text">
                🌱 Lakehouse ESG
            </div>
        </div>
    </div>
    """
    st.markdown(header_html, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        if st.button("🏠 Home", key="btn_home", use_container_width=True):
            st.session_state.page = 'home'
            st.session_state.pop('selected_company', None)
            st.rerun()
    
    with col2:
        if st.button("ℹ️ About", key="btn_about", use_container_width=True):
            st.session_state.page = 'about'
            st.session_state.pop('selected_company', None)
            st.rerun()
    
    with col3:
        if st.button("📧 Contact", key="btn_contact", use_container_width=True):
            st.session_state.page = 'contact'
            st.session_state.pop('selected_company', None)
            st.rerun()
    
    with col4:
        if st.button("📖 Methodology", key="btn_methodology", use_container_width=True):
            st.session_state.page = 'methodology'
            st.session_state.pop('selected_company', None)
            st.rerun()

def render_footer():
    st.markdown("---")
    st.markdown("""
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <div style='text-align: center; padding: 1rem; background: #FFFDE1; border-radius: 10px; border: 2px solid #93BD57;'>
        <p style='margin: 0 0 0.5rem 0; font-weight: 600; color: #93BD57; font-size: 1.1rem;'>Lakehouse ESG</p>
        <div style='margin: 0.5rem 0;'>
            <a href="https://www.facebook.com/people/Data4ESGenius/61577691633847/" target="_blank" 
               style="margin: 0 10px; text-decoration: none; color: #1877f2;">
                <i class="fab fa-facebook"></i> Data4ESGenius
            </a>
            <a href="mailto:data4esgenius@gmail.com" 
               style="margin: 0 10px; text-decoration: none; color: #ea4335;">
                <i class="fas fa-envelope"></i> data4esgenius@gmail.com
            </a>
        </div>
        <p style='margin: 0.5rem 0 0 0; font-size: 0.9rem; color: #666;'>ESG Evaluation Tool © 2025</p>
    </div>
    """, unsafe_allow_html=True)

def render_filters(loader):
    """Render filter section and return selected values"""
    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    st.markdown("### 🔍 Filters & Rankings")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        sectors = loader.get_sectors()
        selected_sector = st.selectbox(
            "Sector",
            ["All"] + sectors,
            index=0,
            key="filter_sector"
        )
    
    with col2:
        industries = loader.get_industries(selected_sector if selected_sector != "All" else None)
        selected_industry = st.selectbox(
            "Industry",
            ["All"] + industries,
            index=0,
            key="filter_industry"
        )
    
    with col3:
        countries = loader.get_countries()
        selected_country = st.selectbox(
            "Country",
            ["All"] + countries,
            index=0,
            key="filter_country"
        )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    return selected_sector, selected_industry, selected_country

def render_score_badge(score, label="Score"):
    """Render color-coded score badge"""
    if score is None or score == 0:
        return f'<span class="score-badge score-low">{label}: N/A</span>'
    
    if score >= 70:
        badge_class = "score-high"
    elif score >= 50:
        badge_class = "score-medium"
    else:
        badge_class = "score-low"
    
    return f'<span class="score-badge {badge_class}">{label}: {score:.0f}</span>'

def render_company_card(company, loader):
    """Render company card with logo and scores - Simplified logo handling"""
    
    with st.container():
        col1, col2, col3 = st.columns([1, 6, 2])
        
        with col1:
            logo_url = company.get('logo_url')
            if logo_url and str(logo_url) not in ['None', 'nan', '', 'NULL']:
                try:
                    st.image(str(logo_url), width=60)
                except Exception as e:
                    st.markdown("🏢")
            else:
                st.markdown("🏢")
        
        with col2:
            st.markdown(f"**{company['company_name']}**")
            st.caption(f"{company.get('sector', 'N/A')} | {company.get('industry', 'N/A')}")
            st.caption(f"📍 {company.get('country', 'N/A')}")
        
        with col3:
            score = company.get('overall_score', 0)
            if score >= 70:
                st.success(f"ESG: {score:.0f}")
            elif score >= 50:
                st.warning(f"ESG: {score:.0f}")
            else:
                st.error(f"ESG: {score:.0f}")
            
            if st.button("View →", key=f"view_{company['company_key']}", use_container_width=True):
                st.session_state.selected_company = company['company_key']
                st.rerun()
        
        st.divider()

def get_coordinates(country, city):
    """Geocode country and city to coordinates"""
    try:
        geolocator = Nominatim(user_agent="lakehouse_esg")
        
        if city and city != 'N/A':
            location_query = f"{city}, {country}"
        else:
            location_query = country
        
        time.sleep(1)
        location = geolocator.geocode(location_query, timeout=10)
        
        if location:
            return location.latitude, location.longitude
        
        time.sleep(1)
        location = geolocator.geocode(country, timeout=10)
        if location:
            return location.latitude, location.longitude
        
        return None, None
        
    except (GeocoderTimedOut, Exception) as e:
        print(f"Geocoding error: {e}")
        return None, None

def render_company_map(country, city, company_name):
    """Render OpenStreetMap for company location"""
    lat, lon = get_coordinates(country, city)
    
    if lat is None or lon is None:
        st.info(f"📍 Location: {city}, {country}" if city else f"📍 Location: {country}")
        return
    
    m = folium.Map(
        location=[lat, lon],
        zoom_start=10 if city else 5,
        tiles="OpenStreetMap"
    )
    
    folium.Marker(
        [lat, lon],
        popup=f"<b>{company_name}</b><br>{city}, {country}" if city else f"<b>{company_name}</b><br>{country}",
        tooltip=company_name,
        icon=folium.Icon(color='green', icon='building', prefix='fa')
    ).add_to(m)
    
    st_folium(m, width=700, height=400)

def render_score_circle(score, label, color="#93BD57"):
    """Render circular score indicator (like Equivest)"""
    if score is None or score == 0:
        score_display = "N/A"
        percentage = 0
    else:
        score_display = f"{score:.0f}"
        percentage = score
    
    circle_html = f"""
    <div style="text-align: center;">
        <div style="
            width: 120px;
            height: 120px;
            border-radius: 50%;
            background: conic-gradient(
                {color} 0deg,
                {color} {percentage * 3.6}deg,
                #E0E0E0 {percentage * 3.6}deg,
                #E0E0E0 360deg
            );
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 1rem;
        ">
            <div style="
                width: 100px;
                height: 100px;
                border-radius: 50%;
                background: white;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 2rem;
                font-weight: bold;
                color: #2C2C2C;
            ">
                {score_display}
            </div>
        </div>
        <div style="font-size: 1.1rem; font-weight: 600; color: #2C2C2C;">
            {label}
        </div>
    </div>
    """
    
    st.markdown(circle_html, unsafe_allow_html=True)

def render_metric_card(icon, value, label, delta=None, delta_color='normal'):
    """Render metric card with icon"""
    delta_html = ""
    if delta is not None:
        color = "#28a745" if delta_color == 'normal' else "#dc3545"
        delta_html = f'<div style="color: {color}; font-size: 0.9rem; margin-top: 0.5rem;">{delta}</div>'
    
    card_html = f"""
    <div style="
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    ">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">{icon}</div>
        <div style="font-size: 1.8rem; font-weight: bold; color: #93BD57; margin-bottom: 0.3rem;">
            {value}
        </div>
        <div style="color: #666; font-size: 0.95rem;">{label}</div>
        {delta_html}
    </div>
    """
    
    st.markdown(card_html, unsafe_allow_html=True)