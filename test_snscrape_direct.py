#!/usr/bin/env python3
"""测试直接使用snscrape（不通过Nitter）"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta, timezone
import snscrape.modules.twitter as sntwitter

def test_snscrape_direct():
    """测试直接snscrape"""
    
    query = "Nothing Phone camera"
    limit = 10
    
    print(f"测试snscrape直接查询: {query}")
    print(f"限制: {limit} 条")
    print("-" * 50)
    
    try:
        scraper = sntwitter.TwitterSearchScraper(query)
        count = 0
        
        for tweet in scraper.get_items():
            if count >= limit:
                break
                
            print(f"{count+1}. [{tweet.date}] @{tweet.user.username}")
            print(f"   内容: {tweet.rawContent[:100]}...")
            print(f"   链接: {tweet.url}")
            print(f"   喜欢: {tweet.likeCount} 转发: {tweet.retweetCount}")
            print()
            
            count += 1
            
        print(f"总计找到: {count} 条推文")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_snscrape_direct()