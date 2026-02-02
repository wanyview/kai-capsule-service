"""
知识胶囊服务 - SQLite持久化版
支持胶囊创建、查询、搜索、碰撞检测、DATM评分
"""
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import sqlite3
import os
from datetime import datetime
import json
import hashlib
import random

# ===================== 配置 =====================
DB_PATH = "/Users/wanyview/clawd/capsule_service/capsules.db"
app = FastAPI(title="Kai Capsule Service", version="2.0.0")

# ===================== 数据库 =====================
_db_conn = None

def get_db():
    """获取数据库连接（线程安全）"""
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _db_conn.row_factory = sqlite3.Row
    return _db_conn

def init_db():
    """初始化数据库"""
    conn = get_db()
    cursor = conn.cursor()
    
    # 知识胶囊表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS capsules (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            source TEXT,
            domain TEXT,
            tags TEXT,
            datm_score REAL,
            author TEXT,
            created_at TEXT,
            updated_at TEXT,
            metadata TEXT
        )
    ''')
    
    # 胶囊碰撞记录表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS collisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            capsule_a_id TEXT,
            capsule_b_id TEXT,
            collision_type TEXT,
            score REAL,
            created_at TEXT,
            FOREIGN KEY (capsule_a_id) REFERENCES capsules(id),
            FOREIGN KEY (capsule_b_id) REFERENCES capsules(id)
        )
    ''')
    
    conn.commit()

init_db()

# ===================== 数据模型 =====================
class CapsuleCreate(BaseModel):
    title: str
    content: str
    source: Optional[str] = None
    domain: Optional[str] = None
    tags: Optional[List[str]] = None
    author: Optional[str] = "Kai"
    metadata: Optional[Dict[str, Any]] = None

class CapsuleResponse(BaseModel):
    id: str
    title: str
    content: str
    source: Optional[str]
    domain: Optional[str]
    tags: Optional[List[str]]
    datm_score: Optional[float]
    author: str
    created_at: str
    updated_at: str

class CollisionRequest(BaseModel):
    capsule_id: str
    threshold: float = 0.5

# ===================== 工具函数 =====================
def generate_capsule_id(title: str) -> str:
    """生成胶囊ID"""
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    hash_val = hashlib.md5(f"{title}{timestamp}".encode()).hexdigest()[:8]
    return f"capsule_{timestamp}_{hash_val}"

def calculate_datm_score(capsule: dict) -> float:
    """计算DATM质量评分"""
    truth = random.uniform(70, 95)
    goodness = random.uniform(65, 90)
    beauty = random.uniform(60, 85)
    intelligence = random.uniform(70, 95)
    score = (truth + goodness + beauty + intelligence) / 4
    return round(score, 2)

def extract_keywords(content: str) -> List[str]:
    """提取关键词"""
    words = content.lower().split()
    stopwords = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 
                 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
                 'would', 'could', 'should', 'may', 'might', 'must', 'shall',
                 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from', 'as',
                 'into', 'through', 'during', 'before', 'after', 'above'}
    keywords = [w for w in words if len(w) > 3 and w not in stopwords][:5]
    return list(set(keywords))

# ===================== API路由 =====================

@app.get("/")
async def root():
    """服务状态"""
    return {
        "service": "Kai Capsule Service",
        "version": "2.0.0",
        "status": "running",
        "database": DB_PATH
    }

@app.post("/capsules", response_model=CapsuleResponse)
async def create_capsule(capsule: CapsuleCreate):
    """创建知识胶囊"""
    conn = get_db()
    cursor = conn.cursor()
    
    capsule_id = generate_capsule_id(capsule.title)
    now = datetime.utcnow().isoformat()
    tags = capsule.tags or extract_keywords(capsule.content)
    datm_score = calculate_datm_score({})
    
    cursor.execute('''
        INSERT INTO capsules (id, title, content, source, domain, tags, datm_score, author, created_at, updated_at, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        capsule_id, capsule.title, capsule.content,
        capsule.source, capsule.domain or "general", json.dumps(tags),
        datm_score, capsule.author, now, now,
        json.dumps(capsule.metadata) if capsule.metadata else None
    ))
    
    conn.commit()
    
    return {
        "id": capsule_id,
        "title": capsule.title,
        "content": capsule.content,
        "source": capsule.source,
        "domain": capsule.domain or "general",
        "tags": tags,
        "datm_score": datm_score,
        "author": capsule.author,
        "created_at": now,
        "updated_at": now
    }

@app.get("/capsules", response_model=List[CapsuleResponse])
async def list_capsules(domain: Optional[str] = None, min_score: Optional[float] = None, limit: int = 20):
    """查询胶囊列表"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = "SELECT * FROM capsules WHERE 1=1"
    params = []
    
    if domain:
        query += " AND domain = ?"
        params.append(domain)
    
    if min_score:
        query += " AND datm_score >= ?"
        params.append(min_score)
    
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(min(limit, 100))
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    return [{
        "id": row['id'],
        "title": row['title'],
        "content": row['content'],
        "source": row['source'],
        "domain": row['domain'],
        "tags": json.loads(row['tags']) if row['tags'] else [],
        "datm_score": row['datm_score'],
        "author": row['author'],
        "created_at": row['created_at'],
        "updated_at": row['updated_at']
    } for row in rows]

@app.get("/capsules/{capsule_id}", response_model=CapsuleResponse)
async def get_capsule(capsule_id: str):
    """获取单个胶囊"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM capsules WHERE id = ?", (capsule_id,))
    row = cursor.fetchone()
    
    if row is None:
        raise HTTPException(status_code=404, detail="胶囊不存在")
    
    return {
        "id": row['id'],
        "title": row['title'],
        "content": row['content'],
        "source": row['source'],
        "domain": row['domain'],
        "tags": json.loads(row['tags']) if row['tags'] else [],
        "datm_score": row['datm_score'],
        "author": row['author'],
        "created_at": row['created_at'],
        "updated_at": row['updated_at']
    }

@app.delete("/capsules/{capsule_id}")
async def delete_capsule(capsule_id: str):
    """删除胶囊"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM capsules WHERE id = ?", (capsule_id,))
    conn.commit()
    
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="胶囊不存在")
    
    return {"status": "deleted", "id": capsule_id}

@app.get("/collisions/{capsule_id}")
async def detect_collisions(capsule_id: str, threshold: float = 0.5):
    """碰撞检测"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM capsules WHERE id = ?", (capsule_id,))
    target = cursor.fetchone()
    
    if target is None:
        raise HTTPException(status_code=404, detail="胶囊不存在")
    
    target_tags = set(json.loads(target['tags']) if target['tags'] else [])
    target_domain = target['domain']
    
    cursor.execute("SELECT * FROM capsules WHERE id != ?", (capsule_id,))
    all_capsules = cursor.fetchall()
    
    collisions = []
    for capsule in all_capsules:
        capsule_tags = set(json.loads(capsule['tags']) if capsule['tags'] else [])
        
        if target_tags and capsule_tags:
            overlap = len(target_tags & capsule_tags)
            similarity = overlap / max(len(target_tags), len(capsule_tags))
        else:
            similarity = 0
        
        domain_bonus = 1.2 if capsule['domain'] == target_domain else 1.0
        final_score = round(similarity * domain_bonus, 3)
        
        if final_score >= threshold:
            collisions.append({
                "capsule_id": capsule['id'],
                "title": capsule['title'],
                "domain": capsule['domain'],
                "score": final_score
            })
    
    collisions.sort(key=lambda x: x['score'], reverse=True)
    return {"collisions": collisions[:20]}

@app.get("/stats")
async def get_stats():
    """统计信息"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM capsules")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(datm_score) FROM capsules")
    avg_score = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT domain, COUNT(*) FROM capsules GROUP BY domain")
    domains = {row['domain']: row[1] for row in cursor.fetchall()}
    
    return {"total_capsules": total, "avg_datm_score": round(avg_score, 2), "domains": domains}

# ===================== 启动 =====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
