#!/usr/bin/env python
#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#https://github.com/quantopian/zipline
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas as pd
import zmq
from bokeh.plotting import figure, output_server, cursession, show

from zipline.api import order, record, symbol, sid, add_history
from zipline.algorithm import TradingAlgorithm
from zipline.protocol import DATASOURCE_TYPE, Event
from zipline.finance.performance.tracker import PerformanceTracker
from zipline.gens.tradesimulation import AlgorithmSimulator
from zipline.finance.slippage import (
    VolumeShareSlippage,
    SlippageModel,
    transact_partial
)


def to_df(dict_):
    data = [o.to_dict() for o in dict_.values()]
    return pd.DataFrame(data)

def show_orders(orders, socket):
    ''' Sends the current order history over a zmq socket
    '''
    df = to_df(orders)
    df['sid'] = df['sid'].apply(lambda o: o.symbol)
    json = df.to_json(date_format='iso')
    socket.send('orders ' + json)

def update_plot(algo):
    ''' Extend this to update a live streaming plot (require bokeh server)
    '''
    # Dive into the 'history_container' how lookbacks are calculated
    hist_df = algo.history(5, '1d', 'price')
    hist_df = hist_df.fill_na(0)
    for sid_id in algo.asset_finder.sids:
        renderer = algo.figure.select(dict(name=str(sid_id)))[0]
        ds = renderer.data_source
        ds.data["y"] = hist_df[sid_id].values
        cursession().store_objects(ds)

#================================================
# Zipline methods
#================================================

def initialize(context):
    # Hack to manually add the assets into the universe so they register with
    # the history container.
    for s in context.asset_finder.sids:
        context._current_universe.add(s)
    add_history(5, '1d', 'price')


def handle_data(context, data):
    # Context is an instance of the Trading algorithm.
    # It's common to attach things on to the instance so they are accessable here.
    # What calls this method?
    # (Hint, it's not the algorithm, look for events and callbacks).
    #order(symbol('AAPL'), 10)
    # Data is of type `BarData`
    if data[symbol('MSFT')].price > 100:
        order(symbol('MSFT'), 10)
    else:
        order(symbol('MSFT'), -10)



class ZMQAlgorithm(TradingAlgorithm):
    def _create_data_generator(self, source_filter, sim_params=None):
        '''
        Allow a client to supply prices at the command line to control the
        algrithms dataset.

        Returns a tuple (timestamp, generator of prices)
        '''
        # This is overriden from the base TradingAlgorithm
        # What else happens in this method?
        # What's the point of the benchmark?
        return self.zmq_event_gen()


    def _create_generator(self, sim_params, source_filter=None):
        """
        Create a basic generator setup using the sources to this algorithm.

        ::source_filter:: is a method that receives events in date
        sorted order, and returns True for those events that should be
        processed by the zipline, and False for those that should be
        skipped.
        """

        if not self.initialized:
            self.initialize(*self.initialize_args, **self.initialize_kwargs)
            self.initialized = True

        if self.perf_tracker is None:
            # HACK: When running with the `run` method, we set perf_tracker to
            # None so that it will be overwritten here.
            #self.perf_tracker = CustomPerfTracker(
            self.perf_tracker = PerformanceTracker(
                sim_params=sim_params, env=self.trading_environment
            )

        self.portfolio_needs_update = True
        self.account_needs_update = True
        self.performance_needs_update = True

        self.data_gen = self._create_data_generator(source_filter, sim_params)

        # Zipline uses a lot of composition object oriented design.
        # How does composition help keep the system flexible and allow for object to be
        # substituted at run-time.
        # Zipline is a backtester that runs on historical data but it's also
        # the engine that drives Quantopion's live trading.
        # What objects might be modified to allow for live trading and greater functionality?
        # What Are the other key objects? (hint, look at the blotter)
        self.trading_client = AlgorithmSimulator(self, sim_params)

        transact_method = transact_partial(self.slippage, self.commission)
        self.set_transact(transact_method)

        # The transform method does the heavy lifting of the main zipline event loop
        return self.trading_client.transform(self.data_gen)


    def zmq_event_gen(self, port=5555):
        context = zmq.Context()
        price_socket = context.socket(zmq.REQ)
        price_socket.connect('tcp://localhost:%s' % port)

        orders_socket = context.socket(zmq.PUB)
        orders_socket.bind('tcp://*:%s' % (port+1))

        for dt in self.sim_params.trading_days:
            prices = []

            # Investigate the asset_finder class. How might data be stored?
            for sid_id in self.trading_environment.asset_finder.sids:
                prompt =  "{}  [{}]".format(dt, sid(sid_id))
                price_socket.send_string(prompt)
                data = price_socket.recv_string()
                price = float(data)

                # Look at DataFrameSource to see that the dataframe input
                # quickly gets turned into a series of events (that are yielded)
                event = {
                    'dt': dt,
                    'sid': sid(sid_id),
                    'price': float(price),
                    'volume': 1e9,
                    'type': DATASOURCE_TYPE.TRADE,
                }
                event = Event(event)
                prices.append(event)

            # We return a generator. Zipline makes heavy use of `yield` and generators
            # to build an event-driven model that runs syncronously].
            # How could we modify an algoithm to run async?
            yield dt, prices

            # What columns are being displayed?
            # Why might some orders have a commission and others are NAN?
            # 'orders' is a collection of all orders placed.
            # How could we change this to include open_orders?
            show_orders(self.blotter.orders, orders_socket)
            # Can we send plots to bokeh?
            # update_plot(self)


class CustomPerfTracker(PerformanceTracker):
    # How are returns calculated?
    # Investigate the PerformanceTracker object and PerformancePeriod objects
    # See how data gets updated in `handle_market_close_daily' or `handle_market_close_minute`
    def handle_simulation_end(self):
        return 'Calculate metrics'

if __name__ == '__main__':
    from datetime import datetime
    import pytz
    from zipline.utils.factory import load_from_yahoo

    # Compare starting an algorithm with a dataframe vs providing 'live' data

    # Set the simulation start and end dates
    # start = datetime(2014, 1, 1, 0, 0, 0, 0, pytz.utc)
    # end = datetime(2014, 11, 1, 0, 0, 0, 0, pytz.utc)

    # Load price data from yahoo.
    # data = load_from_yahoo(stocks=['AAPL'], indexes={}, start=start,
    #                        end=end)

    # Create and run the algorithm.
    symbols = ['AAPL', 'MSFT']

    # Note that the data is blank. This is normally a dataframe.
    data = []
    # Technically run takes a source parameter. run(source)
    # The source will have a start and end that are passed to the trading
    # environment.
    # Could we create a special source? ZMQSource... LiveSource?

    # Set the simulation start and end dates
    start = datetime(2014, 1, 1, 0, 0, 0, 0, pytz.utc)
    end = datetime(2014, 1, 3, 0, 0, 0, 0, pytz.utc)
    algo = ZMQAlgorithm(initialize=initialize,
                            handle_data=handle_data,
                            identifiers=symbols,
                            start=start,
                            end=end,
                        )

    # Create a bokeh plot to get streaming updates
    # Note, there might be some version incompatibilies between versions.
    #
    # prepare output to server
    # output_server("animated_line")
    #
    # fig = figure(plot_width=400, plot_height=400)
    # for i, name in symbols:
    #     fig.line([1, 2, 3, 4, 5], [0, 0, 0, 0, 0], name=str(i))
    # algo.figure = fig
    # show(fig)

    results = algo.run(data)
    print results

    # What kind of objects get returned that can calculate returns and
    # other performance metrics?
    # analyze(results=results)
