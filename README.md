# Sistemas_Distribuidos_TP1 - Cambiano Agustín - 102291

# Client

Se encarga de leer los archivos y enviarlos por socket de a batches, envía también los archivos de categorías de cada país. Utiliza procesos hijos para leer los archivos.

# Accepter

Se encarga de recibir los datos a ser insertados en los pipelines de procesamiento, y de enviar la respuesta final del procesamiento. Realiza el recibimiento de los datos en procesos ya que un único cliente no escalará mucho la cantidad de procesos a conectar si escala la cantidad de datos por archivo. Introduce en las líneas el país del cual es el dato de la línea y la categoría a la que pertenece el video.

# MOM

Es la biblioteca que se encarga de dirigir los mensajes a los containers correspondientes. Lee del archivo de configuración a qué cluster se dirigen los mensajes de cada computadora, retiene información sobre el tipo de conexión que hay entre computadoras (cola normal o publisher/subscriber) y envía mensajes de la forma que corresponda. En el caso de que se envíe a una cola normal simplemente se envía un mensaje, mientras que en el caso de que se envíe con routing se utiliza un hash para determinar a qué computadora de las suscritas a ese exchage particular irá la línea a procesar. Se encarga también de filtrar los datos que no son necesarios para las siguientes etapas de procesamiento.

# Consideraciones generales

- Para el manejo de cuándo debe dejar de recibir mensajes un container se decidió utilizar un conteo de mensajes especiales (en este caso None). Cada container sabe cuántas computadoras tiene la etapa que la precede, por lo que sabe cuántos mensajes de finalización debe recibir antes de saber que no le llegarán más datos para procesar y que debe avisarle a las siguientes etapas que los containers están siendo apagados. 
- El manejo de sigterm se hace de forma similar al apagado normal, enviando un mensaje de fin de ejecución a los siguientes procesos para que estos decidan apagarse. Al enviar todos los contenedores un mensaje a la siguiente etapa, estos también recibirán un mensaje de la etapa que la precede, cerrando así eventualmente el programa siendo ejecutado.
