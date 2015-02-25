/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gob.inamhi.md;

import gob.inamhi.util.Conexion;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Diego
 */
public class ProcessedDataMD {

    private final String base = "bandahm";
    private final String usuario = "postgres";
    private final String password = "inamhidb";
    private final String driver = "org.postgresql.Driver";
    private final String host = "192.168.1.226";
    private final String puerto = "5432";

    public ResultSet datosEstacion(String tabla, int esta, int copa, String fecha) {
        Conexion con = new Conexion(usuario, password, driver, host, puerto, base);
        ResultSet rs = null;
        try {
            // TODO code application logic here
            String columnanomb = tabla.split("\\.")[1];
            con.conectar();
            String query = "select * from " + tabla + " where esta__id=" + esta + " and copa__id=" + copa + " and " + columnanomb + "fetd='" + fecha + "'";
            System.out.println(query);
            rs = con.getSentencia().executeQuery(query);
            return rs;
        } catch (SQLException ex) {
            Logger.getLogger(ProcessedDataMD.class.getName()).log(Level.SEVERE, null, ex);
            return rs;
        } finally {
//            con.desconectar();
        }
    }
}
