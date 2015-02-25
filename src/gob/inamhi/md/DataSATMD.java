/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gob.inamhi.md;

import gob.inamhi.dp.Transferir;
import gob.inamhi.util.Conexion;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Diego
 */
public class DataSATMD {

    private final String base = "SAT";
    private final String usuario = "postgres";
    private final String password = "inamhidb";
    private final String driver = "org.postgresql.Driver";
    private final String host = "192.168.1.226";
    private final String puerto = "5432";

    public boolean buscarRepetido(String tabla, int esta, int copa, String fecha, double valor) {
        Conexion con = new Conexion(usuario, password, driver, host, puerto, base);
        boolean resultado = false;
        try {
            // TODO code application logic here
            con.conectar();
            String query = "select * from " + tabla + " where esta__id=" + esta + " and copa__id=" + copa + " and " + tabla + "fetd='" + fecha + "'and " + tabla + "valo=" + valor + "";
            System.out.println(query);
            ResultSet rs = con.getSentencia().executeQuery(query);
            if (rs.next()) {
                resultado = true;
            }
            return resultado;
        } catch (SQLException ex) {
            Logger.getLogger(Transferir.class.getName()).log(Level.SEVERE, null, ex);
            return resultado;
        } finally {
            con.desconectar();
        }
    }

    public void insertarDato(String tabla, int esta, int copa, String fecha, double valor, String uing) {
        Conexion con = new Conexion(usuario, password, driver, host, puerto, base);
        try {
            // TODO code application logic here
            con.conectar();
            String query = "insert into " + tabla + " (esta__id,copa__id,cali1__id," + tabla + "fetd," + tabla + "valo," + tabla + "uing) values (" + esta + "," + copa + ",8,'" + fecha + "'," + valor + ",'" + uing + "')";
            System.out.println(query);
            con.getSentencia().execute(query);
            con.commit();

        } catch (SQLException ex) {
            Logger.getLogger(Transferir.class.getName()).log(Level.SEVERE, null, ex);
            con.rollback();

        } finally {
            con.desconectar();
        }
    }
}
