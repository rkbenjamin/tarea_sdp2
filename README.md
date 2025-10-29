# Tarea 2 - Resiliencia, procesamiento de flujos y calidad de respuestas

## Descripción del Proyecto
Extensión del sistema distribuido desarrollado en la **Tarea 1**, incorporando **procesamiento asíncrono** mediante **Apache Kafka** y **Apache Flink**.  
El sistema busca **mejorar la tolerancia a fallos** y **optimizar la calidad de las respuestas** generadas por el modelo LLM, implementando un **feedback loop** que permite reintentar respuestas de baja calidad.

## Objetivos Cumplidos

### Requisitos Funcionales
- [x] **Procesamiento Asíncrono con Kafka:** comunicación desacoplada entre productores y consumidores.  
- [x] **Gestión de Errores y Reintentos:** manejo automático de sobrecarga y límites de cuota mediante `retry_manager`.  
- [x] **Procesamiento de Flujos con Flink:** cálculo de score y decisión de reintento para respuestas de baja calidad.  
- [x] **Feedback Loop:** reenvío de consultas con baja calidad al flujo inicial.  
- [x] **Persistencia Asíncrona:** almacenamiento de resultados validados vía Kafka.  
- [x] **Despliegue en Contenedores:** sistema completo orquestado con Docker Compose.

---

## Arquitectura General

```text
Generador → Kafka → LLM Manager → Flink → Almacenamiento
                  ↑                         ↓
            retry_manager  ←  feedback (baja calidad)
```

---

## Flujo del Sistema

1. **g_trafico** genera una pregunta desde el dataset.  
2. Se consulta al **almacenamiento** para verificar si ya existe un resultado previo.  
3. Si no existe, la pregunta se **publica en Kafka** (tópico de preguntas pendientes).  
4. Un **consumidor LLM** obtiene la pregunta, solicita respuesta al modelo y maneja los posibles errores:  
   - *Overload*: reintento exponencial.  
   - *Quota*: reintento diferido.  
5. Las respuestas exitosas se envían a **Flink**, que calcula su *score* usando el servicio **puntaje**.  
6. Si el *score* supera el umbral → se publica en el tópico de resultados válidos y se persiste.  
   Si no → se reinyecta en el flujo de Kafka para reintentar generación.  
7. El módulo **almacenamiento** guarda los resultados finales.

---

## Componentes del Sistema

| Servicio | Puerto | Descripción | Tecnología |
|----------|--------|-------------|-------------|
| **g_trafico** | 5000 | Generador de tráfico (Zipf / Poisson) | Python/Flask |
| **cache** | 5001 | Cache de respuestas LRU/FIFO | Python/Flask |
| **llm_manager** | 5002 | Orquestador de consultas al modelo vía Kafka | Python/Flask |
| **puntaje** | 5003 | Cálculo de similitud coseno (TF-IDF) | Python/Flask/sklearn |
| **almacenamiento** | 5004 | Persistencia y base de datos SQLite | Python/Flask |
| **retry_manager** | — | Controlador de reintentos (overload / quota) | Python |
| **flink_job** | 8081 | Procesamiento de flujos y feedback loop | Apache Flink |
| **kafka / zookeeper** | 9092 / 2181 | Bus de mensajería asíncrono | Apache Kafka |
| **ollama** | 11434 | Servidor de modelos LLM | Ollama |

---

## Instalación y Despliegue

### Prerrequisitos
- Docker y Docker Compose instalados  
- Al menos **4 GB de RAM** (para Ollama)  
- Acceso a internet para descargar imágenes base

---

### 1. Clonar y ejecutar
```bash
git clone <repositorio>
cd tareap2_sd
docker compose up -d --build
