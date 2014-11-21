using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.IO;

namespace ExportBiostar
{
    class Program
    {
        static void Main(string[] args)
        {
            // solo hay un parámetro y es la fecha
            if (args.Count() < 1)
            {
                Console.WriteLine("Número de parámetros insuficiente, introduzca una fecha");
                return;
            }
            // suponemos una fecha en formato dd/mm/aaaa
            string[] e = args[0].Split('/');
            if (e.Length != 3)
            {
                Console.WriteLine("Fecha incorrecta, el formato debe ser dd/mm/aaaa");
                return;
            }
            DateTime fecha;
            try
            {
                fecha = new DateTime(int.Parse(e[2]), int.Parse(e[1]), int.Parse(e[0]));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Fecha incorrecta, el formato debe ser dd/mm/aaaa");
                return;
            }
            // comprobamos si quiere ver la importación, línea por línia
            bool detalle = false;
            string fichero = "";
            if (args.Count() > 1)
            {
                fichero = args[1];
            }

            if (args.Count() > 2)
            {
                detalle = args[2].Equals("v", StringComparison.OrdinalIgnoreCase);
            }
            // calculamos según la fecha donde estaría el inicio y el fin
            // en biostar los marcajes se guardan como segundos desde el 01/01/1970 00:00:00
            DateTime dt_inicio = new DateTime(fecha.Year, fecha.Month, fecha.Day, 0, 0, 0);
            DateTime dt_fin = new DateTime(fecha.Year, fecha.Month, fecha.Day, 23, 59, 59);
            int inicio = ConvertToUnixTimestamp(dt_inicio);
            int fin = ConvertToUnixTimestamp(dt_fin);

            // montamos el nombre del fichero a grabar 
            if (fichero=="") fichero = Directory.GetCurrentDirectory() + String.Format("\\{0:yyyyMMdd}.txt", fecha);
            string linea = ""; // linea de apoyo para grabar en fichero

            Console.WriteLine("Exportando el dia {0:dd/MM/yyyy} al fichero {1}", fecha, fichero);

            // Abrimos la conexión con la base de datos
            // leer la cadena de conexion del config
            var connectionString = ConfigurationManager.ConnectionStrings["BioStar"].ConnectionString;
            // crear la conexion y devolverla.
            using (MySqlConnection conn = new MySqlConnection(connectionString))
            {
                conn.Open();
                MySqlCommand cmd = conn.CreateCommand();
                string sql = @"
                    SELECT
                     tbelg.nDateTime AS MARCAJE,
                     tbelg.nUserID AS USERID0,
                     tbu.sUserID AS USERID1,
                     tbelg.nIsUseTA AS USADOTA
                    FROM tb_event_log AS tbelg
                    LEFT JOIN tb_user AS tbu ON tbu.nUserIdn = tbelg.nUserID
                    WHERE NOT tbu.sUserID IS NULL
                    AND tbelg.nDateTime >= {0} AND tbelg.nDateTime <= {1}
                ";
                sql = String.Format(sql, inicio, fin);
                cmd.CommandText = sql;
                MySqlDataReader rdr = cmd.ExecuteReader();
                if (rdr.HasRows)
                {
                    // abrimos el fichero para grabar las lineas que obtengamos
                    StreamWriter sw = new StreamWriter(fichero);
                    while (rdr.Read())
                    {
                        linea = FromRowToLine(rdr);
                        sw.WriteLine(linea);
                        if (detalle) Console.WriteLine(linea);
                    }
                    sw.Close();
                }
                else
                {
                    Console.WriteLine("No hay registros para la fecha indicada");
                }
                conn.Close();
                Console.WriteLine("Proceso finalizado");
            }
             
        }
        public static DateTime ConvertFromUnixTimestamp(double timestamp)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return origin.AddSeconds(timestamp);
        }

        public static int ConvertToUnixTimestamp(DateTime date)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            TimeSpan diff = date.ToUniversalTime() - origin;
            return (int)Math.Floor(diff.TotalSeconds);
        }

        public static string FromRowToLine(MySqlDataReader rdr)
        {
            string linea = "";
            // obtener del registro leido la fecha.
            int marca = rdr.GetInt32("MARCAJE");
            DateTime f = ConvertFromUnixTimestamp(double.Parse(marca.ToString()));
            // obtener el usuario del marcaje
            int userId = rdr.GetInt32("USERID1");

            // plantilla de la linea del fichero
            // 0 = Código de usuario (5 posiciones)
            // 1 = Mes (2P)
            // 2 = Dia (2P)
            // 3 = Hora (2P)
            // 4 = Minuto (2P)
            // 5,6,7 = Sin significado pero necesarios
            string plantilla = "{0:00000},{1:00},{2:00},{3:00},{4:00},{5:0000},{6:0000},{7:00000}";
            linea = String.Format(plantilla, userId, f.Month, f.Day, f.Hour, f.Minute, 0, 0, 0);
            
            return linea;
        }
    }
}
