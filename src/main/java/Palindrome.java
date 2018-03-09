public class Palindrome {

    public static boolean isPalindrome(String word) {
        String drow = "";
        int l = word.length();
        while (l > 0) {
            drow += word.charAt(l - 1);
            l--;
        }
        return word.equalsIgnoreCase(drow);
    }

    public static void main(String[] args) {
        System.out.println(Palindrome.isPalindrome("Deleveled"));
        System.out.println(Palindrome.isPalindrome("Hello"));
    }
}