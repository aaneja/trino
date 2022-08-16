/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;

class JoinConstraintNode
{
    private Integer traceId;
    private ArrayList<JoinConstraintNode> children;
    private String joinConstraintString;
    private boolean isRoot;

    public JoinConstraintNode()
    {
        traceId = null;
        children = null;
        joinConstraintString = null;
        isRoot = true;
    }

    public JoinConstraintNode(Integer valueParam)
    {
        traceId = valueParam;
        children = null;
        joinConstraintString = null;
        isRoot = true;
    }

    public JoinConstraintNode(ArrayList<JoinConstraintNode> children)
    {
        traceId = null;
        for (JoinConstraintNode child : children) {
            appendChild(child);
        }
        joinConstraintString = null;
    }

    public void appendChild(JoinConstraintNode child)
    {
        if (children == null) {
            children = new ArrayList<JoinConstraintNode>();
        }

        children.add(child);
        child.isRoot = false;
    }

    private String buildJoinConstraintString()
    {
        StringBuilder builder = new StringBuilder();
        if (isRoot == true) {
            builder.append("JOIN");
        }

        if (traceId != null) {
            builder.append(traceId);
        }
        else {
            boolean first = true;
            builder.append("(");
            for (JoinConstraintNode child : children) {
                if (!first) {
                    builder.append(" ");
                }

                String childJoinConstraintString = child.joinConstraintString();
                builder.append(childJoinConstraintString);
                first = false;
            }

            builder.append(")");
        }

        return builder.toString();
    }

    public String joinConstraintString()
    {
        if (joinConstraintString == null) {
            joinConstraintString = buildJoinConstraintString();
        }

        return joinConstraintString;
    }

    private static enum Token
    {LPAR, RPAR, NUMBER, EOF, SPACE};

    private static class Scanner
    {
        private final Reader in;
        private int c;
        private Token token;
        private int number;

        public Scanner(Reader in)
                throws IOException
        {
            this.in = in;
            c = in.read();
        }

        public Token getToken()
        {
            return token;
        }

        public int getNumber()
        {
            return number;
        }

        public Token nextToken()
                throws IOException
        {
            while (c == ' ') {
                c = in.read();
            }

            if (c < 0) {
                token = Token.EOF;
                return token;
            }
            if (c >= '0' && c <= '9') {
                number = c - '0';
                c = in.read();
                while (c >= '0' && c <= '9') {
                    number = 10 * number + (c - '0');
                    c = in.read();
                }

                token = Token.NUMBER;
                return token;
            }

            switch (c) {
                case '(':
                    token = Token.LPAR;
                    break;
                case ')':
                    token = Token.RPAR;
                    break;
                default:
                    throw new RuntimeException("Unknown character " + c);
            }

            c = in.read();
            return token;
        }
    }

    private static ArrayList<JoinConstraintNode> parseList(Scanner scanner)
            throws IOException
    {
        ArrayList<JoinConstraintNode> nodes = new ArrayList<JoinConstraintNode>();
        if (scanner.getToken() != Token.RPAR) {
            nodes.add(parse(scanner));
            while (scanner.getToken() != Token.RPAR) {
                JoinConstraintNode newJoinConstraintNode = parse(scanner);
                nodes.add(newJoinConstraintNode);
            }
        }

        return nodes;
    }

    public static JoinConstraintNode parse(String constraintString)
            throws IOException
    {
        JoinConstraintNode joinConstraintNode = null;
        char[] characters = constraintString.toCharArray();

        int i;
        for (i = 0; i < constraintString.length(); i++) {
            if (!Character.isWhitespace(characters[i])) {
                break;
            }
        }

        if (i < constraintString.length()) {
            if (constraintString.substring(i, i + 4).toUpperCase().equals("JOIN")) {
                constraintString = constraintString.substring(i + 4);
                StringReader in = new StringReader(constraintString);
                Scanner scanner = new Scanner(in);
                scanner.nextToken();
                joinConstraintNode = parse(scanner);
            }
        }

        return joinConstraintNode;
    }

    private static JoinConstraintNode parse(Scanner scanner)
            throws IOException
    {
        switch (scanner.getToken()) {
            case NUMBER:
                int value = scanner.getNumber();
                scanner.nextToken();
                return new JoinConstraintNode(value);
            case LPAR:
                scanner.nextToken();
                ArrayList<JoinConstraintNode> nodes = parseList(scanner);
                if (scanner.getToken() != Token.RPAR) {
                    throw new RuntimeException(") expected");
                }
                scanner.nextToken();
                return new JoinConstraintNode(nodes);
            case EOF:
                return null;
            default:
                throw new RuntimeException("Number or ( expected");
        }
    }
}
