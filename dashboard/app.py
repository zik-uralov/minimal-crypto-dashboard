import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import time
from consumers.dashboard_consumer import data_manager
from config import CRYPTO_SYMBOLS, DASHBOARD_CONFIG
import threading

# Page configuration
st.set_page_config(
    page_title="Real-Time Crypto Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    .alert-card {
        background-color: #fff3cd;
        padding: 0.5rem;
        border-radius: 5px;
        border-left: 5px solid #ffc107;
        margin: 0.2rem 0;
    }
</style>
""", unsafe_allow_html=True)

def start_data_consumer():
    """Start the data consumer in a separate thread"""
    thread = threading.Thread(target=data_manager.start_consuming, daemon=True)
    thread.start()

def format_currency(value):
    """Format number as currency"""
    return f"${value:,.2f}"

def format_percentage(value):
    """Format number as percentage"""
    return f"{value:+.2f}%"

def create_price_chart(metrics_data, symbol):
    """Create a price chart for a symbol"""
    if symbol not in metrics_data or not metrics_data[symbol]:
        return go.Figure()
    
    df = pd.DataFrame(metrics_data[symbol])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['current_price'],
        mode='lines',
        name='Price',
        line=dict(color='#1f77b4', width=2)
    ))
    
    fig.update_layout(
        title=f"{symbol} Price Movement",
        xaxis_title="Time",
        yaxis_title="Price (USDT)",
        height=300,
        showlegend=False
    )
    
    return fig

def create_volume_chart(metrics_data, symbol):
    """Create a volume chart for a symbol"""
    if symbol not in metrics_data or not metrics_data[symbol]:
        return go.Figure()
    
    df = pd.DataFrame(metrics_data[symbol])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['total_volume'],
        name='Volume',
        marker_color='#ff7f0e'
    ))
    
    fig.update_layout(
        title=f"{symbol} Trading Volume",
        xaxis_title="Time",
        yaxis_title="Volume (USDT)",
        height=300,
        showlegend=False
    )
    
    return fig

def main():
    # Header
    st.markdown('<h1 class="main-header">📊 Real-Time Crypto Analytics Dashboard</h1>', 
                unsafe_allow_html=True)
    
    # Start data consumer if not already running
    if not hasattr(st.session_state, 'consumer_started'):
        start_data_consumer()
        st.session_state.consumer_started = True
    
    # Auto-refresh
    if st.button('🔄 Manual Refresh'):
        st.rerun()

    # Get latest data
    data = data_manager.get_latest_data()
    metrics_data = data['metrics_data']
    recent_alerts = data['recent_alerts']
    current_prices = data['current_prices']
    last_update = data.get('last_update')

    # Auto-refresh toggle
    auto_refresh = st.checkbox('Auto-refresh every 2 seconds', value=True)

    if not any(metrics_data.get(symbol) for symbol in CRYPTO_SYMBOLS):
        st.info("Waiting for live market data... hang tight while the stream warms up.")
        if auto_refresh:
            time.sleep(DASHBOARD_CONFIG['update_interval'])
            st.rerun()
        st.stop()
    
    # Overview Section
    st.header("📈 Market Overview")
    if last_update:
        st.caption(f"Last update: {last_update.strftime('%H:%M:%S UTC')}")
    
    # Create columns for metrics
    cols = st.columns(len(CRYPTO_SYMBOLS))
    
    for i, symbol in enumerate(CRYPTO_SYMBOLS):
        with cols[i]:
            if symbol in current_prices:
                latest_metric = metrics_data[symbol][-1] if symbol in metrics_data and metrics_data[symbol] else None
                
                if latest_metric:
                    st.metric(
                        label=symbol,
                        value=format_currency(latest_metric['current_price']),
                        delta=format_percentage(latest_metric['price_change_pct'])
                    )
                    
                    # Additional metrics in expander
                    with st.expander("Details"):
                        st.write(f"**24h Volume:** {format_currency(latest_metric['total_volume'])}")
                        st.write(f"**Volatility:** {latest_metric['volatility']}%")
                        st.write(f"**Trade Count:** {latest_metric['trade_count']}")
                        st.write(f"**Buy/Sell Ratio:** {latest_metric['buy_sell_ratio']:.2f}")
                else:
                    st.metric(label=symbol, value="No data", delta="N/A")
            else:
                st.metric(label=symbol, value="No data", delta="N/A")
    
    # Charts Section
    st.header("📊 Price & Volume Charts")
    
    # Select symbol for detailed charts
    selected_symbol = st.selectbox("Select Symbol for Detailed View", CRYPTO_SYMBOLS)
    
    if selected_symbol in metrics_data and metrics_data[selected_symbol]:
        col1, col2 = st.columns(2)
        
        with col1:
            price_chart = create_price_chart(metrics_data, selected_symbol)
            st.plotly_chart(price_chart, use_container_width=True)
        
        with col2:
            volume_chart = create_volume_chart(metrics_data, selected_symbol)
            st.plotly_chart(volume_chart, use_container_width=True)
    
    # Alerts Section
    st.header("🚨 Recent Alerts")
    
    if recent_alerts:
        for alert in list(recent_alerts)[-10:]:  # Show last 10 alerts
            alert_time = datetime.fromisoformat(alert['timestamp'].replace('Z', '+00:00'))
            time_str = alert_time.strftime("%H:%M:%S")
            
            with st.container():
                st.markdown(f"""
                <div class="alert-card">
                    <strong>{alert['type'].replace('_', ' ').title()}</strong> - {alert['symbol']}<br>
                    {alert['message']}<br>
                    <small>Time: {time_str}</small>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("No recent alerts")
    
    # Raw Data Section
    st.header("🔍 Raw Metrics Data")
    
    if metrics_data:
        all_metrics = []
        for symbol, metrics_list in metrics_data.items():
            for metric in metrics_list[-5:]:  # Last 5 metrics per symbol
                all_metrics.append(metric)
        
        if all_metrics:
            df = pd.DataFrame(all_metrics)
            st.dataframe(df.sort_values('timestamp', ascending=False))
        else:
            st.info("No metrics data available")
    else:
        st.info("Waiting for data...")
    
    if auto_refresh:
        time.sleep(DASHBOARD_CONFIG['update_interval'])
        st.rerun()
    
    # Footer
    st.markdown("---")
    st.markdown(
        "**Real-time Crypto Analytics Dashboard** | "
        f"Last update: {data['last_update'].strftime('%Y-%m-%d %H:%M:%S') if data['last_update'] else 'Never'}"
    )

if __name__ == "__main__":
    main()
