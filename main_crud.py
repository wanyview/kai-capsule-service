#!/usr/bin/env python3
"""
简化版胶囊CRUD API（修正版）
创建时间: 2026-02-02 20:33
更新时间: 2026-02-02 20:37
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import sqlite3
import uuid
from datetime import datetime

app = FastAPI(title="Capsule CRUD API", version="1.0.1")

DB_PATH = "capsules.db"

class CapsuleCreate(BaseModel):
    title: str
    body: str
    tags: List[str]
    author: str = "Kai"

class):
    title: CapsuleUpdate(BaseModel Optional[str] = None
    body: Optional[str] = None
    tags: Optional[List[str]] = None

class Capsule(BaseModel):
    id: str
    title: str
    body: str
    tags: List[str]
    datm_score: Optional[float] = None
    author: str
    created_at: str
    updated_at: str

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/")
def read_root():
    return {"message": "Capsule CRUD API v1.0.1", "status": "running"}

@app.post("/capsules", response_model=Capsule)
def create_capsule(capsule: CapsuleCreate):
    conn = get_db()
    cursor = conn.cursor()
    
    capsule_id = f"capsule_{uuid.uuid4().hex[:8]}"
    now = datetime.utcnow().isoformat()
    tags_str = ",".join(capsule.tags)
    
    # 使用content字段（不是body）
    cursor.execute('''
        INSERT INTO capsules (id, title, content, tags, author, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1)
    ''', (capsule_id, capsule.title, capsule.body, tags_str, capsule.author, now, now))
    
    conn.commit()
    
    result = {
        "id": capsule_id,
        "title": capsule.title,
        "body": capsule.body,
        "tags": capsule.tags,
        "datm_score": None,
        "author": capsule.author,
        "created_at": now,
        "updated_at": now
    }
    
    conn.close()
    return result

@app.get("/capsules", response_model=List[Capsule])
def get_capsules(limit: int = 10):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, title, content, tags, datm_score, author, created_at, updated_at
        FROM capsules ORDER BY created_at DESC LIMIT ?
    ''', (limit,))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            "id": row["id"],
            "title": row["title"],
            "body": row["content"],  # content -> body
            "tags": row["tags"].split(",") if row["tags"] else [],
            "datm_score": row["datm_score"],
            "author": row["author"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"]
        })
    
    conn.close()
    return results

@app.get("/capsules/{capsule_id}", response_model=Capsule)
def get_capsule(capsule_id: str):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, title, content, tags, datm_score, author, created_at, updated_at
        FROM capsules WHERE id = ?
    ''', (capsule_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Capsule not found")
    
    return {
        "id": row["id"],
        "title": row["title"],
        "body": row["content"],  # content -> body
        "tags": row["tags"].split(",") if row["tags"] else [],
        "datm_score": row["datm_score"],
        "author": row["author"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"]
    }

@app.put("/capsules/{capsule_id}", response_model=Capsule)
def update_capsule(capsule_id: str, capsule: CapsuleUpdate):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM capsules WHERE id = ?', (capsule_id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Capsule not found")
    
    now = datetime.utcnow().isoformat()
    updates = []
    params = []
    
    if capsule.title is not None:
        updates.append("title = ?")
        params.append(capsule.title)
    if capsule.body is not None:
        updates.append("content = ?")  # body -> content
        params.append(capsule.body)
    if capsule.tags is not None:
        updates.append("tags = ?")
        params.append(",".join(capsule.tags))
    
    updates.append("updated_at = ?")
    params.append(now)
    params.append(capsule_id)
    
    query = f"UPDATE capsules SET {', '.join(updates)} WHERE id = ?"
    cursor.execute(query, params)
    
    conn.commit()
    
    cursor.execute('SELECT * FROM capsules WHERE id = ?', (capsule_id,))
    row = cursor.fetchone()
    conn.close()
    
    return {
        "id": row["id"],
        "title": row["title"],
        "body": row["content"],
        "tags": row["tags"].split(",") if row["tags"] else [],
        "datm_score": row["datm_score"],
        "author": row["author"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"]
    }

@app.delete("/capsules/{capsule_id}")
def delete_capsule(capsule_id: str):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM capsules WHERE id = ?', (capsule_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Capsule not found")
    
    cursor.execute('DELETE FROM capsules WHERE id = ?', (capsule_id,))
    conn.commit()
    conn.close()
    
    return {"message": "Capsule deleted", "id": capsule_id}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
