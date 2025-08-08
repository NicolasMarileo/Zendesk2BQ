# Imagen base oficial de Python
FROM python:3.12-slim

# Evita prompts interactivos
ENV DEBIAN_FRONTEND=noninteractive

# Crea directorio de trabajo
WORKDIR /app

# Copia los archivos al contenedor
COPY . .

# Instala dependencias del sistema (si necesitas más, agrégalas aquí)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential && \
    pip install --upgrade pip && \
    pip install -r requirements.txt && \
    apt-get remove -y gcc build-essential && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Variable para usar credenciales implícitas en GCP
ENV GOOGLE_APPLICATION_CREDENTIALS=""

# Comando por defecto (puedes cambiarlo si haces otro entrypoint)
CMD ["python", "src/main.py"]