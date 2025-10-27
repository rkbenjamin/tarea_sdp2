#!/usr/bin/env python3
import sqlite3
import sys

try:
    conn = sqlite3.connect('data/results.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            ROUND(AVG(score), 4) as avg_score,
            MIN(score) as min_score,
            MAX(score) as max_score
        FROM results
    """)
    
    total, avg_score, min_score, max_score = cursor.fetchone()
    
    print("METRICAS DE CALIDAD:")
    print(f"Total de registros: {total}")
    print(f"Score promedio: {avg_score}")
    print(f"Score mínimo: {min_score}")
    print(f"Score máximo: {max_score}")
    
    cursor.execute("""
        SELECT 
            CASE 
                WHEN score < 0.2 THEN '0-0.2'
                WHEN score < 0.4 THEN '0.2-0.4'
                WHEN score < 0.6 THEN '0.4-0.6' 
                WHEN score < 0.8 THEN '0.6-0.8'
                ELSE '0.8-1.0'
            END as rango,
            COUNT(*) as cantidad
        FROM results 
        GROUP BY rango 
        ORDER BY rango
    """)
    
    print("\nDISTRIBUCIÓN DE SCORES:")
    for rango, cantidad in cursor.fetchall():
        porcentaje = (cantidad / total) * 100
        print(f"{rango}: {cantidad} registros ({porcentaje:.1f}%)")
    
    conn.close()
    
except Exception as e:
    print(f"error: {e}")
    print("asegúrate de que la base de datos existe en: data/results.db")