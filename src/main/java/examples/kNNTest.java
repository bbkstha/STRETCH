//import org.apache.ignite.Ignite;
//import org.apache.ignite.Ignition;
//import org.apache.ignite.internal.util.IgniteUtils;
//import org.apache.ignite.ml.knn.models.KNNStrategy;
//import org.apache.ignite.ml.knn.models.KNNModel;
//import org.apache.ignite.ml.math.distances.EuclideanDistance;
//import org.apache.ignite.ml.structures.LabeledDataset;
//import org.apache.ignite.ml.structures.LabeledDatasetTestTrainPair;
//import org.apache.ignite.ml.structures.preprocessing.LabeledDatasetLoader;
//import org.apache.ignite.ml.structures.preprocessing.LabellingMachine;
//import org.apache.ignite.thread.IgniteThread;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Path;
//import java.util.Arrays;
//
//public class kNNTest {
//
//        /** Separator. */
//        private static final String SEPARATOR = ",";
//
//        /** Path to the Iris dataset. */
//        private static final String KNN_IRIS_TXT = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/src/main/resources/2.csv";
//
//        /**
//         * Executes example.
//         *
//         * @param args Command line arguments, none required.
//         */
//        public static void main(String[] args) throws InterruptedException {
//            System.out.println(">>> kNN classification example started.");
//            // Start ignite grid.
//            try (Ignite ignite = Ignition.start("/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-fabric-2.2.0-bin/config/default-config.xml")) {
//                System.out.println(">>> Ignite grid started.");
//
//                IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
//                        kNNTest.class.getSimpleName(), () -> {
//
//                    try {
//                        // Prepare path to read
//                        File file = IgniteUtils.resolveIgnitePath(KNN_IRIS_TXT);
//                        if (file == null)
//                            throw new RuntimeException("Can't find file: " + KNN_IRIS_TXT);
//
//                        Path path = file.toPath();
//
//                        // Read dataset from file
//                        LabeledDataset dataset = LabeledDatasetLoader.loadFromTxtFile(path, SEPARATOR, true, false);
//
//                        // Random splitting of iris data as 70% train and 30% test datasets
//                        LabeledDatasetTestTrainPair split = new LabeledDatasetTestTrainPair(dataset, 0.3);
//
//                        System.out.println("\n>>> Amount of observations in train dataset " + split.train().rowSize());
//                        System.out.println("\n>>> Amount of observations in test dataset " + split.test().rowSize());
//
//                        LabeledDataset test = split.test();
//                        LabeledDataset train = split.train();
//
//                        KNNModel knnMdl = new KNNModel(2, new EuclideanDistance(), KNNStrategy.SIMPLE, train);
//
//                        // Clone labels
//                        final double[] labels = test.labels();
//
//                        // Save predicted classes to test dataset
//                        LabellingMachine.assignLabels(test, knnMdl);
//
//                        // Calculate amount of errors on test dataset
//                        int amountOfErrors = 0;
//                        for (int i = 0; i < test.rowSize(); i++) {
//                            if (test.label(i) != labels[i])
//                                amountOfErrors++;
//                        }
//
//                        System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
//                        System.out.println("\n>>> Accuracy " + amountOfErrors / (double)test.rowSize());
//
//                        // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
//                        int[][] confusionMtx = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
//                        for (int i = 0; i < test.rowSize(); i++) {
//                            int idx1 = (int)test.label(i);
//                            int idx2 = (int)labels[i];
//                            confusionMtx[idx1 - 1][idx2 - 1]++;
//                        }
//                        System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtx));
//
//                        // Calculate precision, recall and F-metric for each class
//                        for (int i = 0; i < 3; i++) {
//                            double precision = 0.0;
//                            for (int j = 0; j < 3; j++)
//                                precision += confusionMtx[i][j];
//                            precision = confusionMtx[i][i] / precision;
//
//                            double clsLb = (double)(i + 1);
//
//                            System.out.println("\n>>> Precision for class " + clsLb + " is " + precision);
//
//                            double recall = 0.0;
//                            for (int j = 0; j < 3; j++)
//                                recall += confusionMtx[j][i];
//                            recall = confusionMtx[i][i] / recall;
//                            System.out.println("\n>>> Recall for class " + clsLb + " is " + recall);
//
//                            double fScore = 2 * precision * recall / (precision + recall);
//                            System.out.println("\n>>> F-score for class " + clsLb + " is " + fScore);
//                        }
//
//                    }
//                    catch (IOException e) {
//                        e.printStackTrace();
//                        System.out.println("\n>>> Unexpected exception, check resources: " + e);
//                    } finally {
//                        System.out.println("\n>>> kNN classification example completed.");
//                    }
//
//                });
//
//                igniteThread.start();
//                igniteThread.join();
//            }
//        }
//    }
//
//
//
