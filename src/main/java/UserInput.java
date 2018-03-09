public class UserInput {

    public static class TextInput {

        static String ti = "";

        public void add(Character c) {
            ti += c;
        }

        public String getValue() {
            return ti;
        }
    }

    public static class NumericInput extends TextInput {

        @Override
        public void add(Character c) {
            if (Character.isDigit(c)) ti += c;
        }

    }

    public static void main(String[] args) {
        TextInput input = new NumericInput();
        input.add('1');
        input.add('a');
        input.add('0');
        System.out.println(input.getValue());

    }
}