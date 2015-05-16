package com.dchm.Naive;

import com.dchm.fileIO.HadoopIO;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Created by apirat on 5/3/15 AD.
 *
 * Written by shadowslight
 *
 */

public class PearsonImpact extends FindImpact {
    private static Logger log	= Logger.getLogger(PearsonImpact.class.getName());
    private final String IMPACT_PATH = "result/realTime/Naive_Realtime/";

    public PearsonImpact(HadoopIO hadoopIO, String dataPath, String folder) {
        this.folder = folder;
        this.hadoopIO = hadoopIO;
        this.dataPath = dataPath;
    }

    public Path writeResultFile(String data, String path, String subPath,
                                long filename) throws IOException {
        Path folder = Paths.get(path, subPath).toAbsolutePath().normalize();
        folder = Files.createDirectories(folder);
        Path filePath = folder.resolve(filename+"");
        BufferedWriter out = Files.newBufferedWriter(filePath,StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        out.write(data);
        out.flush();
        out.close();
        return filePath;
    }

    @Override
    public void run() {
        Path filePath;
        long filename = System.currentTimeMillis();
        JSONArray pearson = read_pearson(vmName);
        if(pearson.length() == 0) {
            return;
        }
        JSONArray jsonArrTmp = new JSONArray();
        JSONArray tmp;
        JSONObject jsonObjTmp = null;
        Map<String, Integer> mapPearson = new HashMap<String, Integer>();
        for (int i = 0; i < pearson.length(); i++) {
            try {
                tmp = pearson.getJSONArray(i);
                if (mapPearson.containsKey(tmp.toString())) {
                    mapPearson.put(tmp.toString(),
                            mapPearson.get(tmp.toString()) + 1);
                } else {
                    mapPearson.put(tmp.toString(), new Integer(1));
                }
            } catch (JSONException e) {
                log.error("pearsonMap error", e);
            }

        }
        Map.Entry me;
        Set set = mapPearson.entrySet();
        Iterator i = set.iterator();
        while (i.hasNext()) {
            me = (Map.Entry) i.next();
            try {
                jsonArrTmp = new JSONArray(me.getKey().toString());
                jsonObjTmp = new JSONObject();
                jsonObjTmp.put("pair", jsonArrTmp);
                jsonObjTmp.put("found", Integer.parseInt(me.getValue().toString()));
                jsonObjTmp.put("Total", numberOfFilePearson);
            } catch (JSONException e) {
                log.error("JSON EXCEPTION", e);
            }
        }
        try {
            filePath = writeResultFile(jsonObjTmp.toString(), "data", "Naive_Realtime", filename);
            upload(filePath.toFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void upload(File file) {
        this.hadoopIO.copyFileToHDFS(file, this.dataPath + IMPACT_PATH);
        file.delete();
    }

    private JSONArray read_pearson(String mor) {
        ArrayList<String> fileFS = new ArrayList<String>();
        for (FileStatus in : hadoopIO.listDirectory(folder)) {
            fileFS.add(in.getPath().toString());
        }
        ArrayList<String> data;

        JSONArray ret = new JSONArray();
        JSONArray jsonArray, jsonArrayTmp;
        JSONObject vm1, vm2;
        numberOfFilePearson = fileFS.size();

        for (String f : fileFS) {
            data = hadoopIO.readFileFromHDFS(f);
            for (String str : data) {
                try {
                    jsonArray = new JSONArray(str);
                    vm1 = jsonArray.getJSONObject(0);
                    vm2 = jsonArray.getJSONObject(1);
                    if (vm1.getString("name").equals(mor)
                            || vm2.getString("name").equals(mor)) {
                        if (Double.parseDouble(jsonArray.getJSONObject(2)
                                .getString("Pearson")) > 0.7) {
                            jsonArrayTmp = new JSONArray();
                            if (vm1.getString("name").equals(mor)) {
                                vm1.put("status", "warning");
                            } else {
                                vm1.put("status", "correlate");
                            }
                            if (vm2.get("name").equals(mor)) {
                                vm2.put("status", "warning");
                            } else {
                                vm2.put("status", "correlate");
                            }
                            jsonArrayTmp.put(vm1);
                            jsonArrayTmp.put(vm2);
                            ret.put(jsonArrayTmp);
                        }
                    }
                } catch (JSONException e) {
                    log.error("Read JSON Pearson error", e);
                    return new JSONArray();
                }
            }
        }
        return ret;
    }
}
