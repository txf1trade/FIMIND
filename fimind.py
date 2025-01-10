import os
import typing

import numpy as np
import pandas as pd
import plotly.express as px
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from FinMind.data import DataLoader
from flask import Flask
from loguru import logger


class TreeMap:
    def __init__(self):
        self.token = os.environ.get("FINMIND_API_TOKEN")
        self.html = "初始化~~~"
        self.api = DataLoader()
        self.api.login_by_token(api_token=self.token)
        self.stock_info = self.api.taiwan_stock_info()
        self.data_clean()

    def data_clean(self) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
        logger.info("data_clean")
        self.stock_info.drop(["date", "type"], axis=1, inplace=True)

    def filter_top_5_stock(self, plot_df: pd.DataFrame) -> pd.DataFrame:
        top_df = plot_df[["stock_id", "industry_category", "Trading_Money"]]
        top_df = top_df.sort_values("Trading_Money", ascending=False)
        top_df = top_df.groupby("industry_category").head(5)
        top_df = top_df[["stock_id", "industry_category"]]
        plot_df = top_df.merge(
            plot_df, how="left", on=["stock_id", "industry_category"]
        )
        return plot_df

    def feature_engineer(self, snapshot_df: pd.DataFrame):
        logger.info("feature_engineer")
        last_datetime = max(snapshot_df["date"])
        plot_df = snapshot_df[
            ["stock_id", "total_amount", "change_rate", "close"]
        ]
        plot_df.columns = ["stock_id", "Trading_Money", "漲跌幅%", "close"]
        plot_df = plot_df.merge(self.stock_info, how="inner", on=["stock_id"])
        for col in ["Index", "大盤"]:
            plot_df = plot_df[plot_df["industry_category"] != col]

        index_df = plot_df.groupby(["industry_category"])["Trading_Money"].agg(
            sum
        )
        index_df = index_df.reset_index()
        index_df.columns = ["industry_category", "Index_Trading_Money"]
        plot_df = plot_df.merge(index_df, how="inner", on=["industry_category"])
        plot_df = self.filter_top_5_stock(plot_df)
        plot_df["stock_name"] = (
            plot_df["stock_id"] + " " + plot_df["stock_name"]
        )
        plot_df["spread_rate_label"] = plot_df["漲跌幅%"].astype(str)
        return plot_df, last_datetime

    def plot(self, plot_df: pd.DataFrame, last_datetime: str):
        logger.info("plot")
        fig = px.treemap(
            plot_df,
            path=["industry_category", "stock_name"],
            values="Trading_Money",
            color="漲跌幅%",
            color_continuous_scale=[[0, "green"], [0.5, "white"], [1, "red"]],
            color_continuous_midpoint=0,
            custom_data=["stock_name", "close", "spread_rate_label"],
            title=f"台股交易額X漲跌幅 {last_datetime}",
            width=1350,
            height=900,
        )
        texttemplate = "%{customdata[0]}<br>收盤價 %{customdata[1]}<br>漲跌幅(%) %{customdata[2]}<br>"
        fig.update_traces(
            textposition="middle center",
            textfont_size=24,
            texttemplate=texttemplate,
        )
        # fig.data[0].labels
        fig.data[0]["marker"]["colors"] = np.round(
            fig.data[0]["marker"]["colors"], 2
        )

        html = fig.to_html()
        return html

    def get_snapshot(self) -> pd.DataFrame:
        logger.info("get snapshot")
        url = "https://api.finmindtrade.com/api/v4/taiwan_stock_tick_snapshot"
        parameter = {
            "token": self.token,  # 參考登入，獲取金鑰
        }
        resp = requests.get(url, params=parameter)
        data = resp.json()
        if data["status"] != 200:
            raise Exception(data["msg"])
        df = pd.DataFrame(data["data"])
        return df

    def main(self):
        # load data
        snapshot_df = self.get_snapshot()
        # feature engineer
        plot_df, last_datetime = self.feature_engineer(snapshot_df)
        # plot
        self.html = self.plot(plot_df, last_datetime)


def set_scheduler():
    scheduler = BackgroundScheduler(
        timezone="Asia/Taipei", job_defaults={"max_instances": 1}
    )
    scheduler.add_job(
        id="snapshot",
        func=tree_map.main,
        trigger="cron",
        day_of_week="*",
        hour="*",
        minute="*",
        second="*/5",
    )
    scheduler.start()
    logger.info("scheduler start")


app = Flask(__name__)
tree_map = TreeMap()


@app.route("/", methods=["GET", "POST"])
def submit():
    html = tree_map.html
    return f"""
        <meta http-equiv="refresh" content="1" /> 
        {html}
    """


set_scheduler()
app.run(host="0.0.0.0", debug=True)
