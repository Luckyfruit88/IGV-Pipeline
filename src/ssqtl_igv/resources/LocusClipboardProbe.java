import java.awt.Point;
import java.awt.Robot;
import java.awt.Toolkit;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import javax.swing.JFrame;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

/** Read a native field after screenshot capture; never replace its contents. */
public final class LocusClipboardProbe {
    private static String readAt(int x, int y) throws Exception {
        Toolkit toolkit = Toolkit.getDefaultToolkit();
        String sentinel = "__LOCUS_NOT_OBSERVED_" + UUID.randomUUID() + "__";
        toolkit.getSystemClipboard().setContents(new StringSelection(sentinel), null);
        Robot robot = new Robot();
        robot.setAutoDelay(60);
        robot.mouseMove(x, y);
        robot.mousePress(InputEvent.BUTTON1_DOWN_MASK);
        robot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);
        robot.keyPress(KeyEvent.VK_CONTROL);
        robot.keyPress(KeyEvent.VK_A);
        robot.keyRelease(KeyEvent.VK_A);
        robot.keyRelease(KeyEvent.VK_CONTROL);
        robot.keyPress(KeyEvent.VK_CONTROL);
        robot.keyPress(KeyEvent.VK_C);
        robot.keyRelease(KeyEvent.VK_C);
        robot.keyRelease(KeyEvent.VK_CONTROL);
        String text = sentinel;
        for (int i = 0; i < 20 && sentinel.equals(text); i++) {
            Thread.sleep(50L);
            Object value = toolkit.getSystemClipboard().getData(DataFlavor.stringFlavor);
            text = value == null ? "" : value.toString();
        }
        robot.keyPress(KeyEvent.VK_ESCAPE);
        robot.keyRelease(KeyEvent.VK_ESCAPE);
        robot.keyPress(KeyEvent.VK_TAB);
        robot.keyRelease(KeyEvent.VK_TAB);
        if (sentinel.equals(text)) throw new IllegalStateException("Native field was not observed");
        return text;
    }

    private static void selfTest() throws Exception {
        final String expected = "chr11:123456789-123456890";
        final JFrame[] frame = new JFrame[1];
        final JTextField[] field = new JTextField[1];
        try {
            SwingUtilities.invokeAndWait(() -> {
                frame[0] = new JFrame("IGV locus probe test");
                field[0] = new JTextField(expected);
                frame[0].add(field[0]);
                frame[0].setBounds(30, 30, 160, 80);
                frame[0].setVisible(true);
                frame[0].toFront();
                field[0].requestFocusInWindow();
            });
            new Robot().waitForIdle();
            Thread.sleep(300L);
            Point[] point = new Point[1];
            SwingUtilities.invokeAndWait(() -> point[0] = field[0].getLocationOnScreen());
            String observed = readAt(point[0].x + 20, point[0].y + 12);
            if (!expected.equals(observed)) throw new IllegalStateException("Native text mismatch: " + observed);
            System.out.println("LOCUS_PROBE_SELF_TEST=PASS");
        } finally {
            SwingUtilities.invokeAndWait(() -> { if (frame[0] != null) frame[0].dispose(); });
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "--self-test".equals(args[0])) {
            selfTest();
            return;
        }
        if (args.length != 2) throw new IllegalArgumentException("Expected native field x and y");
        String observed = readAt(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        System.out.print(Base64.getEncoder().encodeToString(observed.getBytes(StandardCharsets.UTF_8)));
    }
}
