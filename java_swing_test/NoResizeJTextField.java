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

import java.awt.Dimension;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

public class NoResizeJTextField {
    public static void main(String[] args) {
        final JFrame frame = new JFrame();  
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);  
        frame.setPreferredSize(new Dimension(500,300));  
        JPanel panel = new JPanel();  
        panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));
        panel.add(new JLabel("Text: "));
        JTextField tf = new JTextField(30);
        tf.setMaximumSize(tf.getPreferredSize());
        tf.setMinimumSize(tf.getPreferredSize());
        panel.add(tf);
        frame.add(panel);   
        SwingUtilities.invokeLater(new Runnable() {             
            public void run() {  
                frame.pack();  
                frame.setVisible(true);  
            }  
        });  
    }
}