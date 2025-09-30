import os
import streamlit as st
import pandas as pd
import sqlalchemy as sa
from typing import Optional
from dotenv import load_dotenv


def get_db_engine() -> sa.Engine:
    # Load variables from .env if present
    load_dotenv()
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")

    url = sa.engine.URL.create(
        drivername="postgresql+psycopg2",
        username=db_user,
        password=db_pass,
        host=db_host,
        port=int(db_port),
        database=db_name,
    )
    return sa.create_engine(url, pool_pre_ping=True)


@st.cache_data(show_spinner=False)
def load_data(limit: Optional[int] = 1000) -> pd.DataFrame:
    engine = get_db_engine()
    query = """
        select time, temperature_2m, relativehumidity_2m, city
        from analytics_staging.stg_deduplicated_data
        order by time asc
    """
    if limit is not None:
        query += f" limit {int(limit)}"
    with engine.begin() as conn:
        df = pd.read_sql_query(sa.text(query), conn, parse_dates=["time"])  # type: ignore[arg-type]
    return df


def main() -> None:
    st.set_page_config(page_title="OpenMeteo Viewer", layout="wide")
    st.title("OpenMeteo: Transformed Data")

    with st.sidebar:
        st.header("Filters")
        row_limit = st.number_input("Max rows", min_value=100, max_value=100000, value=2000, step=100)
        st.markdown("Connection settings (via env vars):")
        st.code("DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS", language="bash")

    try:
        df = load_data(limit=int(row_limit))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Database error: {exc}")
        st.stop()

    if df.empty:
        st.warning("No data found in analytics_staging.stg_deduplicated_data")
        return

    # City filter
    cities = sorted(df["city"].dropna().unique().tolist())
    selected_cities = st.multiselect("City", options=cities, default=cities[:5] if cities else [])
    if selected_cities:
        df = df[df["city"].isin(selected_cities)]

    st.subheader("Table")
    st.dataframe(
        df.sort_values(["city", "time"]).reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Chart: Temperature and Humidity over Time")
    if not df.empty:
        tab1, tab2 = st.tabs(["Temperature", "Humidity"])  # simple switch
        with tab1:
            st.line_chart(
                df.set_index("time")[
                    ["temperature_2m"]
                ],
                use_container_width=True,
            )
        with tab2:
            st.line_chart(
                df.set_index("time")[
                    ["relativehumidity_2m"]
                ],
                use_container_width=True,
            )


if __name__ == "__main__":
    main()


