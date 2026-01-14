import streamlit as st

def show():
    st.markdown("# Contact Us")
    
    st.markdown("""
    We'd love to hear from you! Whether you have questions about our data, suggestions for 
    improvements, or partnership inquiries, feel free to reach out.
    """)
   
    st.markdown("### 📍 Contact Information")
        
    st.markdown("""
        **Data4ESGenius Team**
        
        📧 Email: data4esgenius@gmail.com  
        📱 Facebook: [Data4ESGenius](https://www.facebook.com/people/Data4ESGenius/61577691633847/)  
    """)
    
    st.markdown("---")
    
    st.markdown("### 🤝 Partnership Opportunities")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **Data Providers**
        
        Interested in contributing ESG data to our platform?
        
        We're always looking for high-quality, verified data sources.
        """)
    
    with col2:
        st.markdown("""
        **Research Institutions**
        
        Collaborate on ESG research projects and access our data infrastructure.
        """)
    
    with col3:
        st.markdown("""
        **Technology Partners**
        
        Integrate ESG Lakehouse data into your applications via our API.
        """)
    
    st.markdown("---")
    
    st.markdown("### ❓ Frequently Asked Questions")

    with st.expander("Is the data free to use?"):
        st.markdown("""
        The web interface is free for individual research and non-commercial use. 
        For commercial applications, API access, or bulk data exports, please contact us for pricing.
        """)
    
    with st.expander("How can I report incorrect data?"):
        st.markdown("""
        If you notice any data discrepancies:
        1. Use the contact form above
        2. Specify the company and metric
        3. Provide the correct information if available
        4. Include your source
        
        We review all reports within 48 hours.
        """)
    
    with st.expander("Can I request additional companies or metrics?"):
        st.markdown("""
        Yes! We're continuously expanding our coverage. Submit your requests through 
        the contact form, and we'll prioritize based on demand and data availability.
        """)