
import static etri.model.Common.DEFAULT_FONT;
import java.awt.FlowLayout;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JProgressBar;
import javax.swing.SwingWorker;
import javax.swing.UIManager;

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
public class TestProgress {
     public static void main(String[] args) {
         
        try {
           for (UIManager.LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
               if ("Nimbus".equals(info.getName())) {
                   UIManager.setLookAndFeel(info.getClassName());
                   break;
               }
           }

           UIManager.put("TextField.font", DEFAULT_FONT); 
           UIManager.put("OptionPane.messageFont", DEFAULT_FONT);
           UIManager.put("OptionPane.buttonFont", DEFAULT_FONT);
           UIManager.put("FileChooser.listFont", DEFAULT_FONT);

       } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | 
               javax.swing.UnsupportedLookAndFeelException ex) {
           ex.printStackTrace();
       }
        
        JFrame frame = new JFrame("Test");
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.setLayout(new FlowLayout());
        frame.setLocationRelativeTo(null);

        final JProgressBar jProgressBar = new JProgressBar();
        final JLabel status = new JLabel("Connecting...");
        frame.add(status);
        frame.add("jProgressBar", jProgressBar);

        frame.pack();
        frame.setVisible(true);

        SwingWorker sw = new SwingWorker() {
            @Override
            protected Object doInBackground() throws Exception {
                jProgressBar.setIndeterminate(true);
                Thread.sleep(6000); // Here you should establish connection
                return null;
            }

            @Override
            public void done(){
                jProgressBar.setIndeterminate(false);
                status.setText("Successful");
                jProgressBar.setValue(100); // 100%
            }
        };
         sw.execute();        
    }

}
