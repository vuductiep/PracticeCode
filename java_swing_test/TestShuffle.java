
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/*
 * Program name : mainJFrame.java
 * 
 * Description : Database process of NIPS.
 * 		System			Subsystem		Module(directory)		Block(source file)
 * 		------			----------		-----------------		------------------
 * 		ksb			nips		nonidentifier		mainJFrame.java
 * 
 * Revision history :
 * 		Date			Author			Version No
 * 		------			----------		------------
 * 		24-MAY-2017		Tiep			version 0.1
 * 
 * COPYRIGHT(c) 2017, ETRI
 * 
 */

/**
 *
 * @author tiep
 */
public class TestShuffle {
  public static void main(String args[])
  {
    int[] solutionArray = { 1, 2, 3, 4, 5, 6, 16, 15, 14, 13, 12, 11 };
//    shuffleArray(solutionArray);
//    for (int i = 0; i < solutionArray.length; i++)
//    {
//      System.out.print(solutionArray[i] + " ");
//    }
    System.out.println();
  }

  // Implementing Fisher–Yates shuffle
  static void shuffleArray(int[] ar)
  {
    // If running on Java 6 or older, use `new Random()` on RHS here
    Random rnd = ThreadLocalRandom.current();
    for (int i = ar.length - 1; i > 0; i--)
    {
      int index = rnd.nextInt(i + 1);
      // Simple swap
      int a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }
}