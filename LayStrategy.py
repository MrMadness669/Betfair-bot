import time
import asyncio
import pytz
import betfairlightweight
from flumine import Flumine, clients, BaseStrategy
from flumine.order.trade import Trade
from flumine.order.order import LimitOrder, OrderStatus
from flumine.markets.market import Market
from betfairlightweight.filters import streaming_market_filter
from betfairlightweight.resources import MarketBook
import pandas as pd
import logging
import datetime
from flumine.worker import BackgroundWorker
from flumine.events.events import TerminationEvent
import os
import csv
from flumine.controls.loggingcontrols import LoggingControl
from flumine.order.ordertype import OrderTypes

# Initialize the Telegram bot token
TELEGRAM_BOT_TOKEN = 'yourtoken'
TELEGRAM_CHAT_ID = 'yourid'  # Replace with your chat ID

# Set up the Telegram bot
telegram_bot = None

# Initialize the Telegram bot
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    from telegram import Bot
    telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)

logging.basicConfig(filename='how_to_automate_2.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(lineno)d:%(message)s')

# Credentials to login and logging in
trading = betfairlightweight.APIClient('name', 'password', app_key='appkey')
client = clients.BetfairClient(trading, interactive_login=True)

# Login
framework = Flumine(client=client)


async def send_telegram_message(message):
    try:
        await telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='MarkdownV2')
    except Exception as e:
        logging.error(f"Error sending Telegram message: {e}")


class LayStrategy(BaseStrategy):
    def __init__(self, flumine, market_filter=None):
        super().__init__(market_filter=market_filter)
        self.max_trade_count = 1
        self.max_live_trade_count = 1
        self.regular_stake = 1
        self.total_loss = 0
        self.flumine = flumine
        self.should_exit_loop = False

    async def start(self, flumine):
        await send_telegram_message("Starting strategy 'LayStrategy'")
        print("Starting strategy 'LayStrategy'")

    def send_notification(self, message):
        send_telegram_message(message)

    def check_market_book(self, market: Market, market_book: MarketBook) -> bool:
        return market_book.status != "CLOSED"

    async def place_and_wait(self, trade, market, runner, last_price_traded, stake, timeout=3600):
        order = trade.create_order(
            side="LAY", order_type=LimitOrder(price=last_price_traded, size=stake)
        )
        market.place_order(order)
        bet_id = order.bet_id

        start_time = time.time()
        while time.time() - start_time < timeout:
            if trade.status == "EXECUTION_COMPLETE":
                await send_telegram_message(f"Order executed: Market ID: {market.market_id}, Selection ID: {runner.selection_id}")
                break

            await asyncio.sleep(1)

        while True:
            await asyncio.sleep(1)
            if trade.status_update == "Orders Updated" and trade.bet_id == bet_id:
                break

    def calculate_running_stake(self) -> float:
        if self.total_loss > 0:
            running_stake = self.total_loss / 3 + self.regular_stake
        else:
            running_stake = self.regular_stake
        return running_stake

    def calculate_total_loss(self, result: str, stake: float, odds: float):
        if result == "LOSE":
            loss_amount = -stake
            self.update_loss(loss_amount)

    def update_loss(self, loss_amount: float):
        self.total_loss += loss_amount

    async def process_market_book(self, market: Market, market_book: MarketBook) -> None:
        snapshot_last_price_traded = []
        snapshot_runner_context = []

        for runner in market_book.runners:
            if runner.status == "ACTIVE" and 3 < runner.last_price_traded < 4:
                snapshot_last_price_traded.append([runner.selection_id, runner.last_price_traded])
                runner_context = self.get_runner_context(market.market_id, runner.selection_id, runner.handicap)
                snapshot_runner_context.append([runner_context.selection_id, runner_context.executable_orders,
                                                runner_context.live_trade_count, runner_context.trade_count])

        snapshot_last_price_traded = pd.DataFrame(snapshot_last_price_traded,
                                                  columns=['selection_id', 'last_traded_price'])

        if not snapshot_last_price_traded.empty:
            snapshot_last_price_traded = snapshot_last_price_traded.sort_values(by=['last_traded_price'])
            fav_selection_id = snapshot_last_price_traded['selection_id'].iloc[0]
            logging.info(snapshot_last_price_traded)

            snapshot_runner_context = pd.DataFrame(snapshot_runner_context,
                                                   columns=['selection_id', 'executable_orders', 'live_trade_count',
                                                            'trade_count'])
            logging.info(snapshot_runner_context)

            for runner in market_book.runners:
                if runner.status == "ACTIVE" and market.seconds_to_start < 200 and not market_book.inplay and \
                        runner.selection_id == fav_selection_id and snapshot_runner_context.iloc[:, 1:].sum().sum() == 0:
                    last_price_traded = runner.last_price_traded
                    if last_price_traded is not None:
                        trade = Trade(
                            market_id=market_book.market_id,
                            selection_id=runner.selection_id,
                            handicap=runner.handicap,
                            strategy=self,
                        )
                        await self.place_and_wait(trade, market, runner, last_price_traded, 1)
                        break


logger = logging.getLogger(__name__)


def terminate(context: dict, flumine, today_only: bool = True, seconds_closed: int = 600) -> None:
    markets = list(flumine.markets.markets.values())
    if today_only:
        markets_today = [m for m in markets if
                         m.market_start_datetime.date() == datetime.datetime.now(datetime.timezone.utc).date() and
                         (m.elapsed_seconds_closed is None or
                          (m.elapsed_seconds_closed and m.elapsed_seconds_closed < seconds_closed))]
        market_count = len(markets_today)
    else:
        market_count = len(markets)
    if market_count == 0:
        logger.info("No more markets available, terminating framework")
        flumine.handler_queue.put(TerminationEvent(flumine))


FIELDNAMES = [
    "bet_id", "strategy_name", "market_id", "selection_id", "trade_id", "date_time_placed", "price",
    "price_matched", "size", "size_matched", "profit", "side", "elapsed_seconds_executable", "order_status",
    "market_note", "trade_notes", "order_notes"
]


class LiveLoggingControl(LoggingControl):
    NAME = "BACKTEST_LOGGING_CONTROL"

    def __init__(self, *args, **kwargs):
        super(LiveLoggingControl, self).__init__(*args, **kwargs)
        self._setup()

    def _setup(self):
        if os.path.exists("orders.csv"):
            logging.info("Results file exists")
        else:
            with open("orders.csv", "w") as m:
                csv_writer = csv.DictWriter(m, delimiter=",", fieldnames=FIELDNAMES)
                csv_writer.writeheader()

    def _process_cleared_orders_meta(self, event):
        orders = event.event
        with open("orders.csv", "a") as m:
            for order in orders:
                if order.order_type.ORDER_TYPE == OrderTypes.LIMIT:
                    size = order.order_type.size
                else:
                    size = order.order_type.liability
                if order.order_type.ORDER_TYPE == OrderTypes.MARKET_ON_CLOSE:
                    price = None
                else:
                    price = order.order_type.price
                try:
                    order_data = {
                        "bet_id": order.bet_id,
                        "strategy_name": order.trade.strategy,
                        "market_id": order.market_id,
                        "selection_id": order.selection_id,
                        "trade_id": order.trade.id,
                        "date_time_placed": order.responses.date_time_placed,
                        "price": price,
                        "price_matched": order.average_price_matched,
                        "size": size,
                        "size_matched": order.size_matched,
                        "profit": 0 if not order.cleared_order else order.cleared_order.profit,
                        "side": order.side,
                        "elapsed_seconds_executable": order.elapsed_seconds_executable,
                        "order_status": order.status.value,
                        "market_note": order.trade.market_notes,
                        "trade_notes": order.trade.notes_str,
                        "order_notes": order.notes_str,
                    }
                    csv_writer = csv.DictWriter(m, delimiter=",", fieldnames=FIELDNAMES)
                    csv_writer.writerow(order_data)
                except Exception as e:
                    logger.error(
                        "_process_cleared_orders_meta: %s" % e,
                        extra={"order": order, "error": e},
                    )

        logger.info("Orders updated", extra={"order_count": len(orders)})

    def _process_cleared_markets(self, event):
        cleared_markets = event.event
        for cleared_market in cleared_markets.orders:
            logger.info(
                "Cleared market",
                extra={
                    "market_id": cleared_market.market_id,
                    "bet_count": cleared_market.bet_count,
                    "profit": cleared_market.profit,
                    "commission": cleared_market.commission,
                },
            )


strategy = LayStrategy(
    flumine=framework,
    market_filter=streaming_market_filter(
        event_type_ids=["7"],
        country_codes=["GB"],
        market_types=["WIN"],
    ),
)

framework.add_strategy(strategy)

framework.add_worker(
    BackgroundWorker(
        framework,
        terminate,
        func_kwargs={"today_only": True, "seconds_closed": 1200},
        interval=60,
        start_delay=60,
    )
)

framework.add_logging_control(LiveLoggingControl())

framework.run()