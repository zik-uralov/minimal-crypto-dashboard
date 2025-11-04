import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, timezone
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

def create_price_chart(metrics_data, symbols, use_log_scale=False):
    """Create a price chart for selected symbols."""
    fig = go.Figure()
    palette = px.colors.qualitative.Plotly
    
    for idx, symbol in enumerate(symbols):
        if symbol not in metrics_data or not metrics_data[symbol]:
            continue
        
        df = pd.DataFrame(metrics_data[symbol])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['current_price'],
            mode='lines',
            name=symbol,
            line=dict(color=palette[idx % len(palette)], width=2)
        ))
    
    if not fig.data:
        fig.update_layout(
            title="Price Movement",
            height=300,
        )
        return fig

    fig.update_layout(
        title="Price Movement",
        xaxis_title="Time",
        yaxis_title="Price (USDT)",
        height=300,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_type="log" if use_log_scale else "linear",
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
        st.caption(f"Last update: {last_update.astimezone(timezone.utc).strftime('%H:%M:%S UTC')}")
    
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
    
    col_price, col_volume = st.columns(2)
    
    with col_price:
        selected_symbols = st.multiselect(
            "Select symbols for price comparison",
            CRYPTO_SYMBOLS,
            default=CRYPTO_SYMBOLS[:2]
        )
        use_log_scale = st.checkbox("Use log scale", value=True, help="Helps compare prices across assets with very different magnitudes.")
        
        available_symbols = [
            symbol for symbol in selected_symbols
            if symbol in metrics_data and metrics_data[symbol]
        ]

        if available_symbols:
            price_chart = create_price_chart(metrics_data, available_symbols, use_log_scale=use_log_scale)
            st.plotly_chart(price_chart, use_container_width=True)
        else:
            st.info("Select symbols that have data available.")
    
    with col_volume:
        volume_symbol = st.selectbox("Select symbol for volume view", CRYPTO_SYMBOLS, index=0)
        
        if volume_symbol in metrics_data and metrics_data[volume_symbol]:
            volume_chart = create_volume_chart(metrics_data, volume_symbol)
            st.plotly_chart(volume_chart, use_container_width=True)
        else:
            st.info("Volume data not available for the selected symbol yet.")
    
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
    footer_time = data['last_update'].astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') if data['last_update'] else 'Never'
    st.markdown(
        "**Real-time Crypto Analytics Dashboard** | "
        f"Last update: {footer_time}"
    )

if __name__ == "__main__":
    main()
