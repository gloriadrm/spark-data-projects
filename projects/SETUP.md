# Spark Lab - Entorno de desarrollo

Este documento recoge la configuración necesaria para ejecutar los proyectos de `spark-lab` en un entorno local.

---

# Requisitos

- Python 3.13
- Java 17 (OpenJDK)
- PySpark 4.x
- JupyterLab

---

# Crear el entorno virtual

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

---

# Instalar Java 17

Instalar OpenJDK 17 mediante Homebrew:

```bash
brew install openjdk@17
```

Comprobar la instalación:

```bash
java -version
```

Debe devolver una versión similar a:

```text
openjdk version "17.x.x"
```

---

# Configurar JAVA_HOME

Añadir al archivo `~/.zshrc`:

```bash
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

Recargar la configuración:

```bash
source ~/.zshrc
```

Comprobar:

```bash
echo $JAVA_HOME
java -version
```

---

# Registrar el kernel de Jupyter

Instalar ipykernel:

```bash
pip install ipykernel
```

Registrar el kernel:

```bash
python -m ipykernel install --user \
    --name spark-lab \
    --display-name "spark-lab"
```

Comprobar los kernels disponibles:

```bash
jupyter kernelspec list
```

---

# Configurar JAVA_HOME en el kernel

Editar:

```text
~/Library/Jupyter/kernels/spark-lab/kernel.json
```

Añadir la sección:

```json
"env": {
  "JAVA_HOME": "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
  "PATH": "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin"
}
```

---

# Iniciar JupyterLab

Activar el entorno:

```bash
source .venv/bin/activate
```

Lanzar JupyterLab:

```bash
jupyter lab
```

Seleccionar el kernel:

```
spark-lab
```

---

# Comprobaciones

Verificar que el notebook utiliza Java 17:

```python
import os
import subprocess

print(os.environ.get("JAVA_HOME"))
subprocess.run(["java", "-version"])
```

La salida debe indicar Java 17.

---

# Problemas frecuentes

## ModuleNotFoundError: No module named 'common'

Comprobar que el directorio raíz del proyecto está añadido al `sys.path`.

---

## JAVA_GATEWAY_EXITED

Normalmente indica que PySpark está utilizando una versión incorrecta de Java.

Comprobar:

```python
import os

print(os.environ.get("JAVA_HOME"))
```

Si devuelve `None`, revisar la configuración del kernel (`kernel.json`).

---

## UnsupportedClassVersionError

Este error indica que Spark se está ejecutando con Java 8.

Spark 4.x requiere Java 17.

Verificar:

```bash
java -version
```

Debe mostrarse OpenJDK 17.

---

# Estructura del proyecto

```
spark-lab/
│
├── common/
├── datasets/
├── projects/
│   ├── 01-sales-etl/
│   ├── 02-nyc-taxi/
│   ├── ...
│
├── requirements.txt
└── SETUP.md
```