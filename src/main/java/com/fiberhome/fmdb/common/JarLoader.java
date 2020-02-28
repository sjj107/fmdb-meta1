package com.fiberhome.fmdb.common;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * @Description jar包加载器
 * @Author Administrator
 * @Date 2019/12/27 9:13
 **/
public class JarLoader {

   public static Map<String, Class<?>> customJars = new HashMap<>(16);

   static public List<String> classNameList=new ArrayList<String>();

   public JarLoader() {

   }

   /**
    * 功能描述： 扫描一个文件夹下面的所有jar，不包含子文件夹和子jar
    *
    * @param directoryPath
    * @return
    */
   public static List<String> loadAllJarFromAbsolute(String directoryPath) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

      File directory = new File(directoryPath);

      classNameList.clear();

      // 判断是否为文件夹，如果是文件，则直接用单个jar解析的方法来解析
      if (!directory.isDirectory()) {
         // 添加jar扫描路径
         addUrl(directory);

         loadJarFromAbsolute(directoryPath);
         return null;
      }

      // 如果是文件夹，则需要循环加载当前文件夹下面的所有jar
      Map<String, Class<?>> clazzMap = new HashMap<>(16);
      File[] jars = directory.listFiles();
      if (jars != null && jars.length > 0) {
         List<String> jarPath = new LinkedList<>();
         for (File file : jars) {
            String fPath = file.getPath();

            // 只加载jar
            if (fPath.endsWith(".jar")) {
               addUrl(file);
               jarPath.add(fPath);
            }
         }

         if (jarPath.size() > 0) {
            for (String path : jarPath) {
               loadJarFromAbsolute(path);
            }
         }
      }
      return classNameList;
   }

   /**
    * 功能描述：添加需要扫描的jar包
    *
    * @param jarPath
    * @throws NoSuchMethodException
    * @throws MalformedURLException
    * @throws InvocationTargetException
    * @throws IllegalAccessException
    */
   private static void addUrl(File jarPath) throws NoSuchMethodException, MalformedURLException, InvocationTargetException, IllegalAccessException {
      URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();

      // 反射获取类加载器中的addURL方法，并将需要加载类的jar路径
      Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      if (!method.isAccessible()) {
         method.setAccessible(true);
      }

      URL url = jarPath.toURI().toURL();

      // 把当前jar的路径加入到类加载器需要扫描的路径
      method.invoke(classLoader, url);
   }

   /**
    * 功能描述：从绝对路径中加载jar包中的类
    * 扫描指定jar包前需要将这个jar的地址加入了系统类加载器的扫描列表中
    * 注意，这里只支持单个jar的加载，如果这个jar还引入了其他jar依赖，会加载失败
    * 所以只能用来加载对其他jar包没有依赖的简单对象类信息
    *
    * @param path
    * @return
    */
   public static void loadJarFromAbsolute(String path) throws IOException {

      JarFile jar = new JarFile(path);
      Enumeration<JarEntry> entryEnumeration = jar.entries();
      while (entryEnumeration.hasMoreElements()) {
         JarEntry entry = entryEnumeration.nextElement();

         // 先获取类的名称，符合条件之后再做处理，避免处理不符合条件的类
         String clazzName = entry.getName();
         if (clazzName.endsWith(".class")) {
            // 去掉文件名后缀
            clazzName = clazzName.substring(0, clazzName.length() - 6);
            // 替换分隔符
            clazzName = clazzName.replace("/", ".");

            classNameList.add(clazzName);

            // 若已包含加载的类，则继续
            if (customJars.containsKey(clazzName)) {
               continue;
            }

            // 否则加载类，如果失败直接跳过
            try {
               Class<?> clazz = Class.forName(clazzName);

               // 将类名作为key，类Class对象作为值存入map
               // 因为类名存在重复的可能，所以这里的类名带包名
               customJars.put(clazzName, clazz);

            } catch (ClassNotFoundException e) {
               e.printStackTrace();
            }

         }
      }
   }
}
