package org.demo.scripts;

public class TestClass {
  public static void main(String a[]) {
    String string = ("You are awesome honey");
    for (int i = 0; i < string.length(); i++)
      System.out.println(i + "->" + string.charAt(i));
  }
}
