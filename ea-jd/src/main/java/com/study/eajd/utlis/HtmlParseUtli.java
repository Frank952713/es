package com.study.eajd.utlis;

import com.study.eajd.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class HtmlParseUtli {

    public static void main(String[] args) throws Exception {
        new HtmlParseUtli().parseJD("java").forEach(System.out::println);
    }

    public List<Content> parseJD(String keywords) throws Exception {
        //获取请求 https://search.jd.com/Search?keyword=java&enc=utf-8&wq=java&pvid=78a45dd46fba4d23a469fb01ac2990e4

//        String url = "https://search.jd.com/Search?keyword=java";

//        String urlKeywords = URLEncoder.encode(keywords, "UTF-8");

        String url = "https://search.jd.com/search?keyword="+keywords+"&enc=utf-8";

        //解析网页,获取网页数据（ Document就是 js的 Document）
        Document document = Jsoup.parse(new URL(url), 30000);


        Element goodListElement = document.getElementById("J_goodsList");

//        System.out.println("==========================================");
//        System.out.println(goodListElement.html());
//        System.out.println("==========================================");
        //获取所有的li元素
        Elements elements = goodListElement.getElementsByTag("li");

        ArrayList<Content> goodList = new ArrayList<>();

        //el就是每一个li标签
        for (Element el : elements) {

            Content content = new Content();

            String title=el.getElementsByClass("p-name").eq(0).text();
            String price=el.getElementsByClass("p-price").eq(0).text().split("￥")[1];

            content.setTitle(title);
            content.setPrice(price);

            //多图片网站，延迟加载（懒加载），data-lazy-img
            content.setImg(el.getElementsByTag("img").eq(0).attr("data-lazy-img"));

            goodList.add(content);

        }
        return goodList;
    }

}
