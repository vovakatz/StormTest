package com.katzstudio;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

public class yfSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            //Stock stock = YahooFinance.get("MSFT");
            //StockQuote quote = stock.getQuote();
            //BigDecimal price = quote.getPrice();
            //BigDecimal prevClose = quote.getPreviousClose();
            Timestamp now = new Timestamp((System.currentTimeMillis()));

            collector.emit(new Values("MSFT", sdf.format(now),12, 10));
        } catch(Exception ex) {
            System.out.println("-------------------" + ex.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("company","timestamp","price","prev_close"));
    }
}
