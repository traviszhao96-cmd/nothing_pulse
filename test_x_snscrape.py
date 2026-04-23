#!/usr/bin/env python3
"""测试X snscrape收集器"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta, timezone
from nt_cam_pulse.fetchers.x_snscrape import XSnscrapeCollector

def test_snscrape():
    """测试snscrape收集器"""
    
    # 模拟配置
    config = {
        "query": '(Nothing Phone) (camera OR photo OR video OR 相机 OR 拍照 OR 评测)',
        "limit": 10,
        "timeout_seconds": 30,
        "nitter_instances": ["https://nitter.net", "https://nitter.poast.org", "https://nitter.1d4.us"],
        "include_keywords": ["nothing", "camera", "photo", "video", "相机", "拍照", "评测"]
    }
    
    # 产品关键词
    product_keywords = ["Nothing Phone", "phone 4a", "phone 4a pro"]
    
    # 创建收集器
    collector = XSnscrapeCollector(
        name="x_snscrape",
        config=config,
        product_keywords=product_keywords
    )
    
    # 设置时间范围（最近7天）
    since = datetime.now(timezone.utc) - timedelta(days=7)
    
    print("开始测试X snscrape收集器...")
    print(f"查询: {config['query']}")
    print(f"时间范围: 从 {since}")
    print("-" * 50)
    
    try:
        # 获取数据
        items = collector.fetch(since)
        
        print(f"找到 {len(items)} 条推文:")
        print("-" * 50)
        
        for i, item in enumerate(items, 1):
            print(f"{i}. [{item.published_at.strftime('%Y-%m-%d %H:%M')}] {item.title}")
            print(f"   作者: {item.author or '未知'}")
            print(f"   URL: {item.url}")
            print(f"   内容预览: {item.content[:100]}...")
            print()
            
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_snscrape()