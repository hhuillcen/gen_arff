/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package genarff;
import java.io.*;
import java.sql.*;
/**
 *
 * @author Herwin
 */
public class Genarff {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
  Connection conexion = null;
        try {
            // Cargar el driver
            Class.forName("com.mysql.jdbc.Driver");
            conexion = DriverManager.getConnection("jdbc:mysql://localhost/weewx", "weewx", "7s3r");
            Statement s = conexion.createStatement();
            ResultSet rs = s.executeQuery("select from_unixtime(dateTime) as fecha,date_format(from_unixtime(dateTime),'%k')+(date_format(from_unixtime(dateTime),'%i'))/60 as hora,round(((outTemp-32)/1.8),2) as temp, outHumidity as humedad, round((windSpeed/0.62137),2) as viento, rain as lluvia, uv as uv_actual,"+
          //      "(select UV from archive as UVA "+
                "(select round(((outTemp-32)/1.8),2) from archive as TT "+
                "where from_unixtime(dateTime)=DATE_ADD(fecha,INTERVAL 2 day) "+
                "order by from_unixtime(dateTime) limit 1 "+
                ") as temp_predict "+
                "from archive "+
                " where  from_unixtime(dateTime)>=date_sub(now(),INTERVAL 3 day) "+
                "order by fecha");

                FileWriter miArchivo = null;
                 PrintWriter escribirArchivo;

                    try
                    {
                        miArchivo = new FileWriter("d:\\arffs\\wekatemp02d+48h.arff");
                        //miArchivo = new FileWriter("/home/hv/public_html/climand13.arff");
                        escribirArchivo = new PrintWriter(miArchivo);
                        //escribirArchivo.println("\""+rs.getString("fecha")+"\""+","+rs.getString("mes")+","+rs.getString("d_anio")+","+rs.getString("hormin")+"," +rs.getString("hora")+","+rs.getString("minuto")+","+rs.getString("temp")+","+rs.getString("humedad")+","+rs.getString("viento")+","+rs.getString("lluvia")+","+rs.getString("uv")+","+rs.getString("UV_M_1D")+","+rs.getString("UV_M_1H"));
                        escribirArchivo.println("@relation wekatemp02d+48");
                        escribirArchivo.println("@attribute hora_minuto REAL");
                        escribirArchivo.println("@attribute temperatura REAL");
                        escribirArchivo.println("@attribute humedad REAL");
                        escribirArchivo.println("@attribute viento REAL");
                        escribirArchivo.println("@attribute lluvia REAL");
                        escribirArchivo.println("@attribute uv REAL");
                        escribirArchivo.println("@attribute temp_predecida REAL");
                        escribirArchivo.println("@data");
                        
                    }
                    catch (Exception ex)
                    {
                        System.out.println(ex.getMessage());
                    }
                    finally
                    {
                        try
                        {
                            if (null != miArchivo)
                            {
                                miArchivo.close();
                            }
                        }
                        catch (Exception ex1)
                        {
                            System.out.println(ex1.getMessage());
                        }
                    }
             
            
            
            
            while (rs.next()) {
                //System.out.println(rs.getString("fecha") + " " + rs.getString("hora")+" "+rs.getString("temp")+" "+rs.getString("humedad")+" "+rs.getString("viento")+" "+rs.getString("uv"));
                 //FileWriter miArchivo = null;
                 //PrintWriter escribirArchivo;

                    try
                    {
                        //miArchivo = new FileWriter("d:\\arffs\climand.txt");
                        miArchivo = new FileWriter("d:\\arffs\\wekatemp02d+48h.arff",true);
                        //miArchivo = new FileWriter("/home/hv/public_html/climand13.arff",true);
                        escribirArchivo = new PrintWriter(miArchivo);
                        //escribirArchivo.println("\""+rs.getString("fecha")+"\""+","+rs.getString("mes")+","+rs.getString("d_anio")+","+rs.getString("hormin")+"," +rs.getString("hora")+","+rs.getString("minuto")+","+rs.getString("temp")+","+rs.getString("humedad")+","+rs.getString("viento")+","+rs.getString("lluvia")+","+rs.getString("uv")+","+rs.getString("UV_M_1D")+","+rs.getString("UV_M_1H"));
                        if ((rs.getString("temp").equalsIgnoreCase("null"))||(rs.getString("temp_predict").equalsIgnoreCase("null")) )
                        {
                            rs.next();
                        }
                        else
                        {
                        escribirArchivo.println(rs.getString("hora")+","+rs.getString("temp")+","+rs.getString("humedad")+","+rs.getString("viento")+","+rs.getString("lluvia")+","+rs.getString("uv_actual")+","+rs.getString("temp_predict"));
                    }   }
                    catch (Exception ex)
                    {
                        System.out.println(ex.getMessage());
                    }
                    finally
                    {
                        try
                        {
                            if (null != miArchivo)
                            {
                                miArchivo.close();
                            }
                        }
                        catch (Exception ex1)
                        {
                            System.out.println(ex1.getMessage());
                        }
                    }
                
                
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } finally { // Se cierra la conexión con la base de datos.
            try {
                if (conexion != null) {
                    conexion.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }        
    }
       
    }
}
