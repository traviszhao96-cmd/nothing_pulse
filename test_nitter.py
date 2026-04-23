#!/usr/bin/env python3
"""测试Nitter实例是否可用"""

import requests
import feedparser
from datetime import datetime, timedelta, timezone

def test_nitter_instance(instance_url, query):
    """测试单个Nitter实例"""
    print(f"测试实例: {instance_url}")
    
    rss_url = f"{instance_url}/search/rss"
    params = {"f": "tweets", "q": query}
    
    try:
        response = requests.get(
            rss_url,
            params=params,
            timeout=10,
            headers={"User-Agent": "NothingCameraPulse/1.0"}
        )
        print(f"  状态码: {response.status_code}")
        
        if response.status_code == 200:
            feed = feedparser.parse(response.text)
            print(f"  RSS条目数: {len(feed.entries)}")
            
            # 显示前几个条目
            for i, entry in enumerate(feed.entries[:3], 1):
                title = getattr(entry, 'title', '无标题')
                link = getattr(entry, 'link', '无链接')
                published = getattr(entry, 'published', '无日期')
                print(f"  {i}. {title[:80]}...")
                print(f"     链接: {link}")
                print(f"     时间: {published}")
                print()
            return len(feed.entries)
        else:
            print(f"  错误: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"  错误: {e}")
    
    return 0

def main():
    """主函数"""
    instances = [
        "https://nitter.net",
        "https://nitter.poast.org", 
        "https://nitter.1d4.us",
        "https://nitter.privacydev.net",
        "https://nitter.fdn.fr"
    ]
    
    queries = [
        "Nothing Phone",
        "Nothing Phone camera",
        "Nothing Phone 相机"
    ]
    
    for query in queries:
        print(f"\n{'='*60}")
        print(f"查询: {query}")
        print('='*60)
        
        total_entries = 0
        for instance in instances:
            entries = test_nitter_instance(instance, query)
            total_entries += entries
            
        print(f"总计找到: {total_entries} 条推文")

if __name__ == "__main__":
    main()