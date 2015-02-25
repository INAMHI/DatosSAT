/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gob.inamhi.dp;

import gob.inamhi.md.DataSATMD;
import gob.inamhi.md.ProcessedDataMD;
import gob.inamhi.util.FechasUtiles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Diego
 */
public class Transferir {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws SQLException {

        String tablaOrigen = "processed_data.data1h";
        String tablaDestino = "data";
        FechasUtiles fu = new FechasUtiles();
//        String[] payaminoAJNapo = new String[]{"30", "23", "112"};      //esta__id,esta__id(SAT),copa__id,cfges__id(SAT) 
//        String[] cocaEnSanSebastian = new String[]{"34", "22", "112"};
//        String[] tenaChaupishungo = new String[]{"11", "33", "68"};
//        String[] narupa = new String[]{"27", "36", "68"};
//        String[] cuyuja = new String[]{"29", "26", "68"};
//        String[] loreto = new String[]{"17", "28", "68"};

//        String[] chacapamba = new String[]{"30", "23", "68"};      //esta__id,esta__id(SAT),copa__id,cfges__id(SAT) 
//        String[] cochahuma = new String[]{"34", "22", "68"};
//        String[] chipla = new String[]{"11", "33", "68"};
//        String[] escudilla = new String[]{"27", "36", "68"};
//        String[] suscalpamba = new String[]{"29", "26", "68"};
//        String[] caniarPtoInca = new String[]{"17", "28", "112"};
        String[] elTablon_lasCuevas = new String[]{"63718", "7", "68"};      //esta__id,esta__id(SAT),copa__id,cfges__id(SAT) 
        String[] laLampada = new String[]{"63719", "8", "68"};
        String[] gualleturo = new String[]{"63721", "11", "68"};
        String[] lasLajas = new String[]{"9", "40", "68"};


        ProcessedDataMD dataMD = new ProcessedDataMD();
        DataSATMD dataSAT = new DataSATMD();

        List<String[]> estaciones = new ArrayList<>();
//        estaciones.add(payaminoAJNapo);
//        estaciones.add(cocaEnSanSebastian);
//        estaciones.add(tenaChaupishungo);
//        estaciones.add(narupa);
//        estaciones.add(cuyuja);
//        estaciones.add(loreto);
//        estaciones.add(chacapamba);
//        estaciones.add(cochahuma);
//        estaciones.add(escudilla);
//        estaciones.add(suscalpamba);
//        estaciones.add(caniarPtoInca);
//        estaciones.add(chipla);
        estaciones.add(elTablon_lasCuevas);
        estaciones.add(laLampada);
        estaciones.add(gualleturo);
        estaciones.add(lasLajas);
        for (String[] estacion : estaciones) {
            int estaProcessed = Integer.parseInt(estacion[0]);
            int estaSAT = Integer.parseInt(estacion[1]);
            int copa = Integer.parseInt(estacion[2]);
//            int cfges = Integer.parseInt(estacion[3]);
            String fechaHora = fu.getFechaYHoraActual();

            String fechaFin = fechaHora;                       //Fecha Actual de ejecución de la tarea
            String fechaInicio = fu.getFechaYHoraMenosUnaHoraAnterior(fechaHora, "12_h");   //Fecha Actual de ejecución de la tarea menos el tiempo de diferencia (prpmhorc)
//            String fechaFinPaso = fu.getFechaYHoraMasUnaHoraPosterior(fechaInicio, "1_h");
//            String fechaInicioAux = fechaInicio;
//            String fechaFinPasoAux = fechaFinPaso;


            do {
                ResultSet rs = dataMD.datosEstacion(tablaOrigen, estaProcessed, copa, fechaInicio);
                while (rs.next()) {
                    double valor = rs.getDouble("data1hvalo");
                    if (!dataSAT.buscarRepetido(tablaDestino, estaSAT, copa, fechaInicio, valor)) {
                        dataSAT.insertarDato(tablaDestino, estaSAT, copa, fechaInicio, valor, "ADMIN");
                    }
                }

                fechaInicio = fu.getFechaYHoraMasUnaHoraPosterior(fechaInicio, "1_h");
//            } while (fechaFin.compareToIgnoreCase(fechaInicioAux) > 0);
            } while (fechaFin.compareToIgnoreCase(fechaInicio) >= 0);
        }
    }
}
